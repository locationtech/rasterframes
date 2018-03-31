/*
 * Copyright 2017 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package astraea.spark.rasterframes.extensions

import java.time.ZonedDateTime

import astraea.spark.rasterframes.util._
import astraea.spark.rasterframes.{MetadataKeys, RasterFrame}
import geotrellis.proj4.CRS
import geotrellis.raster.resample.{Bilinear, ResampleMethod}
import geotrellis.raster.{CellGrid, MultibandTile, ProjectedRaster, Tile, TileLayout}
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.tiling.{LayoutDefinition, Tiler}
import geotrellis.util.{LazyLogging, MethodExtensions}
import geotrellis.vector.ProjectedExtent
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{Metadata, TimestampType}
import spray.json._

import scala.reflect.runtime.universe._

/**
 * Extension methods on [[RasterFrame]] type.
 * @since 7/18/17
 */
trait RasterFrameMethods extends MethodExtensions[RasterFrame]
  with RFSpatialColumnMethods with MetadataKeys with LazyLogging {
  import Implicits.{WithDataFrameMethods, WithRasterFrameMethods}

  private val _stableDF = self
  import _stableDF.sqlContext.implicits._


  /**
   * A convenience over `DataFrame.withColumnRenamed` whereby the `RasterFrame` type is maintained.
   */
  def withRFColumnRenamed(existingName: String, newName: String): RasterFrame =
    (self: DataFrame).withColumnRenamed(existingName, newName).certify

  /** Get the spatial column. */
  def spatialKeyColumn: TypedColumn[Any, SpatialKey] = {
    val key = self.findSpatialKeyField
    key
      .map(_.name)
      .map(col(_).as[SpatialKey])
      .getOrElse(throw new IllegalArgumentException("All RasterFrames must have a column tagged with context"))
  }

  /**
   * Reassemble the [[TileLayerMetadata]] record from DataFrame metadata.
   */
  def tileLayerMetadata: Either[TileLayerMetadata[SpatialKey], TileLayerMetadata[SpaceTimeKey]] = {
    val spatialMD = self.findSpatialKeyField
      .map(_.metadata)
      .getOrElse(throw new IllegalArgumentException(s"RasterFrame operation requsted on non-RasterFrame: $self"))

    if (self.findTemporalKeyField.nonEmpty)
      Right(extract[TileLayerMetadata[SpaceTimeKey]](CONTEXT_METADATA_KEY)(spatialMD))
    else
      Left(extract[TileLayerMetadata[SpatialKey]](CONTEXT_METADATA_KEY)(spatialMD))
  }

  /** Get the CRS covering the RasterFrame. */
  def crs: CRS = tileLayerMetadata.fold(_.crs, _.crs)

  /** Add a temporal key to the RasterFrame, assigning the same temporal key to all rows. */
  def addTemporalComponent(value: TemporalKey): RasterFrame = {
    require(self.temporalKeyColumn.isEmpty, "RasterFrame already has a temporal component")
    val tlm = tileLayerMetadata.left.get
    val newTlm = tlm.map(k ⇒ SpaceTimeKey(k, value))

    // I wish there was a better way than this....
    // can't do `lit(value)` because you get
    // "Unsupported literal type class geotrellis.spark.TemporalKey" error
    val litKey = udf(() ⇒ value)

    val df = self.withColumn(TEMPORAL_KEY_COLUMN.columnName, litKey())

    df.setSpatialColumnRole(SPATIAL_KEY_COLUMN, newTlm)
      .setTemporalColumnRole(TEMPORAL_KEY_COLUMN)
      .certify
  }

  /** Create a temporal key from the given time and assign it as thea temporal key for all rows. */
  def addTemporalComponent(value: ZonedDateTime): RasterFrame = addTemporalComponent(TemporalKey(value))

  /**
   * Append a column containing the temporal key rendered as a TimeStamp.
   * @param colName name of column to add
   * @return updated RasterFrame
   */
  def withTimestamp(colName: String = TIMESTAMP_COLUMN.columnName): RasterFrame = {
    self.withColumn(colName, (TEMPORAL_KEY_COLUMN.getField("instant").as[Long] / 1000).cast(TimestampType))
      .certify
  }

  /**
   * Perform a spatial join between two raster frames. Currently ignores a temporal column if there is one.
   * The left TileLayerMetadata is propagated to the result.
   *
   * **WARNING: This is a work in progress, and only works if both raster frames have the same
   * tile layer metadata. A more flexible spatial join is in the works.**
   *
   * @param right Right side of the join.
   * @param joinType One of: `inner`, `outer`, `left_outer`, `right_outer`, `leftsemi`.
   */
  @Experimental
  def spatialJoin(right: RasterFrame, joinType: String = "inner"): RasterFrame = {
    val left = self

    val leftMetadata = left.tileLayerMetadata.widen
    val rightMetadata = right.tileLayerMetadata.widen

    if (leftMetadata.layout != rightMetadata.layout) {
      logger.warn(
        "Multi-layer query assumes same tile layout. Differences detected:\n\t" +
          leftMetadata + "\nvs.\n\t" + rightMetadata
      )
    }

    def updateNames(rf: RasterFrame,
                    prefix: String,
                    sk: TypedColumn[Any, SpatialKey],
                    tk: Option[TypedColumn[Any, TemporalKey]]) = {
      tk.combine(rf: DataFrame)((t, rf) ⇒ rf.withColumnRenamed(t.columnName, prefix + t.columnName))
        .withColumnRenamed(sk.columnName, prefix + sk.columnName)
        .certify
    }

    val preppedLeft = updateNames(left, "left_", left.spatialKeyColumn, left.temporalKeyColumn)
    val preppedRight = updateNames(right, "right_", right.spatialKeyColumn, right.temporalKeyColumn)

    val leftSpatialKey = preppedLeft.spatialKeyColumn
    val leftTemporalKey = preppedLeft.temporalKeyColumn
    val rightSpatialKey = preppedRight.spatialKeyColumn
    val rightTemporalKey = preppedRight.temporalKeyColumn

    val spatialPred = leftSpatialKey === rightSpatialKey
    val temporalPred = leftTemporalKey.flatMap(l ⇒ rightTemporalKey.map(r ⇒ l === r))

    val joinPred = temporalPred.map(t ⇒ spatialPred && t).getOrElse(spatialPred)

    val joined = preppedLeft.join(preppedRight, joinPred, joinType)

    val result = if (joinType == "inner") {
      // Undo left renaming and drop right
      val spatialFix = joined
        .withColumnRenamed(leftSpatialKey.columnName, left.spatialKeyColumn.columnName)
        .drop(rightSpatialKey.columnName)

      left.temporalKeyColumn.tupleWith(leftTemporalKey).combine(spatialFix) {
        case ((orig, updated), rf) ⇒ rf
          .withColumnRenamed(updated.columnName, orig.columnName)
          .drop(rightTemporalKey.get.columnName)
      }
    } else joined
    result.certify
  }

  /**
   * Performs a full RDD scans of the key column for the data extent, and updates the [[TileLayerMetadata]] data extent to match.
   */
  def clipLayerExtent: RasterFrame = {
    val metadata = tileLayerMetadata
    val extent = metadata.widen.extent
    val layout = metadata.widen.layout
    val trans = layout.mapTransform

    def updateBounds[T: SpatialComponent: Boundable: JsonFormat: TypeTag](tlm: TileLayerMetadata[T],
                                                                          keys: Dataset[T]): DataFrame = {
      val keyBounds = keys
        .map(k ⇒ KeyBounds(k, k))
        .reduce(_ combine _)

      val gridExtent = trans(keyBounds.toGridBounds())
      val newExtent = gridExtent.intersection(extent).getOrElse(gridExtent)
      self.setSpatialColumnRole(self.spatialKeyColumn, tlm.copy(extent = newExtent, bounds = keyBounds))
    }

    val df = metadata.fold(
      tlm ⇒ updateBounds(tlm, self.select(self.spatialKeyColumn)),
      tlm ⇒ {
        updateBounds(
          tlm,
          self
            .select(self.spatialKeyColumn, self.temporalKeyColumn.get)
            .map { case (s, t) ⇒ SpaceTimeKey(s, t) }
        )
      }
    )

    df.certify
  }

  /**
   * Convert a single tile column from RasterFrame to a GeoTrellis [[TileLayerRDD]]
   * @param tileCol column with tiles to be the
   */
  def toTileLayerRDD(tileCol: Column): Either[TileLayerRDD[SpatialKey], TileLayerRDD[SpaceTimeKey]] =
    tileLayerMetadata.fold(
      tlm ⇒ Left(ContextRDD(self.select(self.spatialKeyColumn, tileCol.as[Tile]).rdd, tlm)),
      tlm ⇒ {
        val rdd = self
          .select(self.spatialKeyColumn, self.temporalKeyColumn.get, tileCol.as[Tile])
          .rdd
          .map { case (sk, tk, v) ⇒ (SpaceTimeKey(sk, tk), v) }
        Right(ContextRDD(rdd, tlm))
      }
    )

  /** Convert all the tile columns in a Rasterrame to a GeoTrellis [[MultibandTileLayerRDD]] */
  def toMultibandTileLayerRDD: Either[MultibandTileLayerRDD[SpatialKey], MultibandTileLayerRDD[SpaceTimeKey]] =
    toMultibandTileLayerRDD(self.tileColumns: _*)

  /** Convert the specified tile columns in a Rasterrame to a GeoTrellis [[MultibandTileLayerRDD]] */
  def toMultibandTileLayerRDD(tileCols: Column*): Either[MultibandTileLayerRDD[SpatialKey], MultibandTileLayerRDD[SpaceTimeKey]] =
    tileLayerMetadata.fold(
      tlm ⇒ {
        val rdd = self
          .select(self.spatialKeyColumn, array(tileCols: _*)).as[(SpatialKey, Array[Tile])]
          .rdd
          .map { case (sk, tiles) ⇒
            (sk, MultibandTile(tiles))
          }
        Left(ContextRDD(rdd, tlm))
      },
      tlm ⇒ {
        val rdd = self
          .select(self.spatialKeyColumn, self.temporalKeyColumn.get, array(tileCols: _*)).as[(SpatialKey, TemporalKey, Array[Tile])]
          .rdd
          .map { case (sk, tk, tiles) ⇒ (SpaceTimeKey(sk, tk), MultibandTile(tiles)) }
        Right(ContextRDD(rdd, tlm))
      }
    )

  /** Extract metadata value. */
  private[rasterframes] def extract[M: JsonFormat](metadataKey: String)(md: Metadata) =
    md.getMetadata(metadataKey).json.parseJson.convertTo[M]

  // TODO: Take care of DRY below
//  private def rasterize[T <: CellGrid: TypeTag](
//    tileCols: Seq[Column],
//    rasterCols: Int,
//    rasterRows: Int,
//    resampler: ResampleMethod): ProjectedRaster[T] = {
//
//    val clipped = clipLayerExtent
//
//    val md = clipped.tileLayerMetadata.widen
//    val newLayout = LayoutDefinition(md.extent, TileLayout(1, 1, rasterCols, rasterRows))
//
//    val trans = md.mapTransform
//
//    //val cellType = rdd.first()._2.cellType
//    val keyBounds = Bounds(SpatialKey(0, 0), SpatialKey(0, 0))
//    val newLayerMetadata =
//      md.copy(layout = newLayout, bounds = keyBounds)
//
//
//    val newLayer = typeOf[T] match {
//      case tpe if tpe <:< typeOf[Tile] ⇒
//        val r = clipped.toTileLayerRDD(tileCols.head)
//          .fold(identity, _.map { case (stk, t) ⇒ (stk.spatialKey, t) }) // <-- Drops the temporal key outright
//          .map { case (key, tile) ⇒ (ProjectedExtent(trans(key), md.crs), tile) }
//        ContextRDD(r, md)
//          .tileToLayout(newLayerMetadata, Tiler.Options(resampler))
//      case tpe if tpe <:< typeOf[MultibandTile] ⇒
//        val r = clipped.toMultibandTileLayerRDD(tileCols: _*)
//          .fold(identity, _.map { case (stk, t) ⇒ (stk.spatialKey, t) }) // <-- Drops the temporal key outright
//          .map { case (key, tile) ⇒ (ProjectedExtent(trans(key), md.crs), tile) }
//        ContextRDD(r, md)
//          .tileToLayout(newLayerMetadata, Tiler.Options(resampler))
//    }
//
//    val stitchedTile = newLayer.stitch()
//
//    val croppedTile = stitchedTile.crop(rasterCols, rasterRows)
//
//    ProjectedRaster(croppedTile, md.extent, md.crs)
//  }

  /** Convert the tiles in the RasterFrame into a single raster. For RasterFrames keyed with temporal keys, they
    * will be merge undeterministically. */
  def toRaster(tileCol: Column,
               rasterCols: Int,
               rasterRows: Int,
               resampler: ResampleMethod = Bilinear): ProjectedRaster[Tile] = {

    val clipped = clipLayerExtent

    val md = clipped.tileLayerMetadata.widen
    val trans = md.mapTransform
    val newLayout = LayoutDefinition(md.extent, TileLayout(1, 1, rasterCols, rasterRows))

    val rdd = clipped.toTileLayerRDD(tileCol)
      .fold(identity, _.map{ case(stk, t) ⇒ (stk.spatialKey, t) }) // <-- Drops the temporal key outright

    val cellType = rdd.first()._2.cellType

    val newLayerMetadata =
      md.copy(layout = newLayout, bounds = Bounds(SpatialKey(0, 0), SpatialKey(0, 0)), cellType = cellType)

    val newLayer = rdd
      .map {
        case (key, tile) ⇒
          (ProjectedExtent(trans(key), md.crs), tile)
      }
      .tileToLayout(newLayerMetadata, Tiler.Options(resampler))

    val stitchedTile = newLayer.stitch()

    val croppedTile = stitchedTile.crop(rasterCols, rasterRows)

    ProjectedRaster(croppedTile, md.extent, md.crs)
  }

  /** Convert the Red, Green & Blue assigned tiles in the RasterFrame into a single color composite raster.
   * For RasterFrames keyed with temporal keys, they will be merged underterministically. */
  def toMultibandRaster(
    tileCols: Seq[Column],
    rasterCols: Int,
    rasterRows: Int,
    resampler: ResampleMethod = Bilinear): ProjectedRaster[MultibandTile] = {

    val clipped = clipLayerExtent

    val md = clipped.tileLayerMetadata.widen
    val trans = md.mapTransform
    val newLayout = LayoutDefinition(md.extent, TileLayout(1, 1, rasterCols, rasterRows))

    val rdd = clipped.toMultibandTileLayerRDD(tileCols: _*)
      .fold(identity, _.map{ case(stk, t) ⇒ (stk.spatialKey, t)}) // <-- Drops the temporal key outright

    val cellType = rdd.first()._2.cellType

    val newLayerMetadata =
      md.copy(layout = newLayout, bounds = Bounds(SpatialKey(0, 0), SpatialKey(0, 0)), cellType = cellType)

    val newLayer = rdd
      .map {
        case (key, tile) ⇒
          (ProjectedExtent(trans(key), md.crs), tile)
      }
      .tileToLayout(newLayerMetadata, Tiler.Options(resampler))

    val stitchedTile = newLayer.stitch()

    val croppedTile = stitchedTile.crop(rasterCols, rasterRows)

    ProjectedRaster(croppedTile, md.extent, md.crs)
  }
}
