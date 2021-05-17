/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017-2019 Astraea, Inc. & Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     [http://www.apache.org/licenses/LICENSE-2.0]
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.locationtech.rasterframes.datasource.geotrellis

import java.io.UnsupportedEncodingException
import java.net.URI
import java.sql.{Date, Timestamp}
import java.time.{ZoneOffset, ZonedDateTime}

import com.typesafe.scalalogging.Logger
import geotrellis.layer.{Metadata => LMetadata, _}
import geotrellis.raster.{CellGrid, MultibandTile, Tile, TileFeature}
import geotrellis.spark.store.{FilteringLayerReader, LayerReader}
import geotrellis.spark.util.KryoWrapper
import geotrellis.store._
import geotrellis.store.avro.AvroRecordCodec
import geotrellis.util._
import geotrellis.vector._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.rf.TileUDT
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, sources}
import org.locationtech.jts.geom
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.datasource.geotrellis.GeoTrellisRelation.{C, TileFeatureData}
import org.locationtech.rasterframes.datasource.geotrellis.TileFeatureSupport._
import org.locationtech.rasterframes.rules.SpatialFilters.{Contains => sfContains, Intersects => sfIntersects}
import org.locationtech.rasterframes.rules.TemporalFilters.{BetweenDates, BetweenTimes}
import org.locationtech.rasterframes.rules.{SpatialRelationReceiver, splitFilters}
import org.locationtech.rasterframes.util.JsonCodecs._
import org.locationtech.rasterframes.util.SubdivideSupport._
import org.locationtech.rasterframes.util._
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * A Spark SQL `Relation` over a standard GeoTrellis layer.
 */
case class GeoTrellisRelation(sqlContext: SQLContext,
  uri: URI,
  layerId: LayerId,
  numPartitions: Option[Int] = None,
  failOnUnrecognizedFilter: Boolean = false,
  tileSubdivisions: Option[Int] = None,
  filters: Seq[Filter] = Seq.empty)
  extends BaseRelation with PrunedScan with SpatialRelationReceiver[GeoTrellisRelation] {

  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  /** Create new relation with the give filter added. */
  def withFilter(value: Filter): GeoTrellisRelation =
    copy(filters = filters :+ value)
  /** Check to see if relation already exists in this. */
  def hasFilter(filter: Filter): Boolean = filters.contains(filter)

  @transient
  private lazy val attributes = AttributeStore(uri)

  @transient
  private lazy val (keyType, tileClass) = attributes.readHeader[LayerHeader](layerId) |>
    (h ⇒ {
      val kt = Class.forName(h.keyClass) match {
        case c if c.isAssignableFrom(classOf[SpaceTimeKey]) ⇒ typeOf[SpaceTimeKey]
        case c if c.isAssignableFrom(classOf[SpatialKey]) ⇒ typeOf[SpatialKey]
        case c ⇒ throw new UnsupportedOperationException("Unsupported key type " + c)
      }
      val tt = Class.forName(h.valueClass) match {
        case c if c.isAssignableFrom(classOf[Tile]) ⇒ typeOf[Tile]
        case c if c.isAssignableFrom(classOf[MultibandTile]) ⇒ typeOf[MultibandTile]
        case c if c.isAssignableFrom(classOf[TileFeature[_, _]]) ⇒ typeOf[TileFeature[Tile, _]]
        case c ⇒ throw new UnsupportedOperationException("Unsupported tile type " + c)
      }
      (kt, tt)
    })

  @transient
  lazy val tileLayerMetadata: Either[TileLayerMetadata[SpatialKey], TileLayerMetadata[SpaceTimeKey]] =
    keyType match {
      case t if t =:= typeOf[SpaceTimeKey] ⇒ Right(
        attributes.readMetadata[TileLayerMetadata[SpaceTimeKey]](layerId)
      )
      case t if t =:= typeOf[SpatialKey] ⇒ Left(
        attributes.readMetadata[TileLayerMetadata[SpatialKey]](layerId)
      )
    }

  def subdividedTileLayerMetadata: Either[TileLayerMetadata[SpatialKey], TileLayerMetadata[SpaceTimeKey]] = {
    tileSubdivisions.filter(_ > 1) match {
      case None ⇒ tileLayerMetadata
      case Some(divs) ⇒ tileLayerMetadata
        .right.map(_.subdivide(divs))
        .left.map(_.subdivide(divs))
    }
  }


  /** This unfortunate routine is here because the number bands in a multiband layer isn't written
   * in the metadata anywhere. This is potentially an expensive hack, which needs further quantifying of impact.
   * Another option is to force the user to specify the number of bands. */
  private lazy val peekBandCount = {
    implicit val sc = sqlContext.sparkContext
    tileClass match {
      case t if t =:= typeOf[MultibandTile] ⇒
        val reader = keyType match {
          case k if k =:= typeOf[SpatialKey] ⇒
            LayerReader(uri).read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](layerId)
          case k if k =:= typeOf[SpaceTimeKey] ⇒
            LayerReader(uri).read[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]](layerId)
        }
        // We're counting on `first` to read a minimal amount of data.
        val tile = reader.first()
        tile._2.bandCount
      case _ ⇒ 1
    }
  }

  override def schema: StructType = {
    val skSchema = ExpressionEncoder[SpatialKey]().schema

    val skMetadata = subdividedTileLayerMetadata.
      fold(_.asColumnMetadata, _.asColumnMetadata) |>
      (Metadata.empty.append.attachContext(_).tagSpatialKey.build)

    val keyFields = keyType match {
      case t if t =:= typeOf[SpaceTimeKey] ⇒
        val tkSchema = ExpressionEncoder[TemporalKey]().schema
        val tkMetadata = Metadata.empty.append.tagTemporalKey.build
        List(
          StructField(C.SK, skSchema, nullable = false, skMetadata),
          StructField(C.TK, tkSchema, nullable = false, tkMetadata),
          StructField(C.TS, TimestampType, nullable = false)
        )
      case t if t =:= typeOf[SpatialKey] ⇒
        List(
          StructField(C.SK, skSchema, nullable = false, skMetadata)
        )
    }

    val tileFields = tileClass match {
      case t if t =:= typeOf[Tile]  ⇒
        List(
          StructField(C.TL, new TileUDT, nullable = true)
        )
      case t if t =:= typeOf[MultibandTile] ⇒
        for(b ← 1 to peekBandCount) yield
          StructField(C.TL + "_" + b, new TileUDT, nullable = true)
      case t if t =:= typeOf[TileFeature[Tile, _]] ⇒
        List(
          StructField(C.TL, new TileUDT, nullable = true),
          StructField(C.TF, DataTypes.StringType, nullable = true)
        )
    }

    val extentField = StructField(C.EX, org.apache.spark.sql.jts.JTSTypes.PolygonTypeInstance, true)
    StructType((keyFields :+ extentField) ++ tileFields)
  }

  type BLQ[K, T] = BoundLayerQuery[K, TileLayerMetadata[K], RDD[(K, T)] with LMetadata[TileLayerMetadata[K]]]

  def applyFilter[K: Boundable: SpatialComponent, T](query: BLQ[K, T], predicate: Filter): BLQ[K, T] = {
    predicate match {
      // GT limits disjunctions to a single type
      // case sources.Or(sfIntersects(C.EX, left), sfIntersects(C.EX, right)) ⇒
      //   query.where(LayerFilter.Or(
      //     Intersects(Extent(left.getEnvelopeInternal)),
      //     Intersects(Extent(right.getEnvelopeInternal))
      //   ))
      // case sfIntersects(C.EX, rhs: geom.Point) ⇒
      //   query.where(Contains(rhs))
      // case sfContains(C.EX, rhs: geom.Point) ⇒
      //   query.where(Contains(rhs))
      // case sfIntersects(C.EX, rhs) ⇒
      //   query.where(Intersects(Extent(rhs.getEnvelopeInternal)))
      case _ ⇒
        val msg = "Unable to convert filter into GeoTrellis query: " + predicate
        if(failOnUnrecognizedFilter)
          throw new UnsupportedOperationException(msg)
        else
          logger.warn(msg + ". Filtering defered to Spark.")
        query
    }
  }

  def applyFilterTemporal[K: Boundable: SpatialComponent: TemporalComponent, T](q: BLQ[K, T], predicate: Filter): BLQ[K, T] = {
    def toZDT(ts: Timestamp) = ZonedDateTime.ofInstant(ts.toInstant, ZoneOffset.UTC)
    def toZDT2(date: Date) = ZonedDateTime.ofInstant(date.toInstant, ZoneOffset.UTC)

    predicate match {
      case sources.EqualTo(C.TS, ts: Timestamp) ⇒
        q.where(At(toZDT(ts)))
      // case BetweenTimes(C.TS, start: Timestamp, end: Timestamp) ⇒
      //   q.where(Between(toZDT(start), toZDT(end)))
      // case BetweenDates(C.TS, start: Date, end: Date) ⇒
      //   q.where(Between(toZDT2(start), toZDT2(end)))
      case _ ⇒ applyFilter(q, predicate)
    }
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    logger.trace(s"Reading: $layerId from $uri")
    logger.trace(s"Required columns: ${requiredColumns.mkString(", ")}")
    logger.trace(s"Filters: $filters")

    implicit val sc = sqlContext.sparkContext
    val reader = LayerReader(uri)

    val columnIndexes = requiredColumns.map(schema.fieldIndex)
    tileClass match {
      case t if t =:= typeOf[Tile] ⇒ query[Tile](reader, columnIndexes)
      case t if t =:= typeOf[TileFeature[Tile, _]] ⇒
        val baseSchema = attributes.readSchema(layerId)
        val schema = scala.util.Try(baseSchema
            .getField("pairs").schema()
            .getElementType
            .getField("_2").schema()
            .getField("data").schema()
        ).getOrElse(
          throw new UnsupportedEncodingException("Embedded TileFeature schema is of unknown/unexpected structure: " + baseSchema.toString(true))
        )
        implicit val codec = GeoTrellisRelation.tfDataCodec(KryoWrapper(schema))
        query[TileFeature[Tile, TileFeatureData]](reader, columnIndexes)
      case t if t =:= typeOf[MultibandTile] ⇒ query[MultibandTile](reader, columnIndexes)
    }
  }

  private def subdivider[K: SpatialComponent, T <: CellGrid[Int]: WithCropMethods](divs: Int) = (p: (K, T)) ⇒ {
    val newKeys = p._1.subdivide(divs)
    val newTiles = p._2.subdivide(divs)
    newKeys.zip(newTiles)
  }

  private def query[T <: CellGrid[Int]: WithCropMethods: WithMergeMethods: AvroRecordCodec: ClassTag](reader: FilteringLayerReader[LayerId], columnIndexes: Seq[Int]) = {
    subdividedTileLayerMetadata.fold(
      // Without temporal key case
      (tlm: TileLayerMetadata[SpatialKey]) ⇒ {

        val parts = numPartitions.getOrElse(reader.defaultNumPartitions)

        val query = splitFilters(filters).foldLeft(
          reader.query[SpatialKey, T, TileLayerMetadata[SpatialKey]](layerId, parts)
        )(applyFilter(_, _))

        val rdd = tileSubdivisions.filter(_ > 1) match {
          case Some(divs) ⇒
            query.result.flatMap(subdivider[SpatialKey, T](divs))
          case None ⇒ query.result
        }

        val trans = tlm.mapTransform
        rdd
          .map { case (sk: SpatialKey, tile: T) ⇒
            val entries = columnIndexes.map {
              case 0 ⇒ sk
              case 1 ⇒ trans.keyToExtent(sk).toPolygon()
              case 2 ⇒ tile match {
                case t: Tile ⇒ t
                case t: TileFeature[Tile @unchecked, TileFeatureData @unchecked] ⇒ t.tile
                case m: MultibandTile ⇒ m.bands.head
              }
              case i if i > 2 ⇒ tile match {
                case t: TileFeature[Tile @unchecked, TileFeatureData @unchecked] ⇒ t.data
                case m: MultibandTile ⇒ m.bands(i - 2)
              }
            }
            Row(entries: _*)
          }
      }, // With temporal key case
      (tlm: TileLayerMetadata[SpaceTimeKey]) ⇒ {
        val trans = tlm.mapTransform

        val parts = numPartitions.getOrElse(reader.defaultNumPartitions)

        val query = splitFilters(filters).foldLeft(
          reader.query[SpaceTimeKey, T, TileLayerMetadata[SpaceTimeKey]](layerId, parts)
        )(applyFilterTemporal(_, _))

        val rdd = tileSubdivisions.filter(_ > 1) match {
          case Some(divs) ⇒
            query.result.flatMap(subdivider[SpaceTimeKey, T](divs))
          case None ⇒ query.result
        }

        rdd
          .map { case (stk: SpaceTimeKey, tile: T) ⇒
            val sk = stk.spatialKey
            val entries = columnIndexes.map {
              case 0 ⇒ sk
              case 1 ⇒ stk.temporalKey
              case 2 ⇒ new Timestamp(stk.temporalKey.instant)
              case 3 ⇒ trans.keyToExtent(stk).toPolygon()
              case 4 ⇒ tile match {
                case t: Tile ⇒ t
                case t: TileFeature[Tile @unchecked, TileFeatureData @unchecked] ⇒ t.tile
                case m: MultibandTile ⇒ m.bands.head
              }
              case i if i > 4 ⇒ tile match {
                case t: TileFeature[Tile @unchecked, TileFeatureData @unchecked] ⇒ t.data
                case m: MultibandTile ⇒ m.bands(i - 4)
              }
            }
            Row(entries: _*)
          }
      }
    )
  }
  // TODO: Is there size speculation we can do?
  override def sizeInBytes = {
    super.sizeInBytes
  }

}

object GeoTrellisRelation {
  /** A dummy type used as a stand-in for ignored TileFeature data. */
  type TileFeatureData = String

  /** Constructor for Avro codec for TileFeature data stand-in. */
  private def tfDataCodec(dataSchema: KryoWrapper[Schema]) = new AvroRecordCodec[TileFeatureData]() {
    def schema: Schema = dataSchema.value
    def encode(thing: TileFeatureData, rec: GenericRecord): Unit = ()
    def decode(rec: GenericRecord): TileFeatureData = rec.toString
  }

  object C {
    lazy val SK = SPATIAL_KEY_COLUMN.columnName
    lazy val TK = TEMPORAL_KEY_COLUMN.columnName
    lazy val TS = TIMESTAMP_COLUMN.columnName
    lazy val TL = TILE_COLUMN.columnName
    lazy val TF = TILE_FEATURE_DATA_COLUMN.columnName
    lazy val EX = GEOMETRY_COLUMN.columnName
  }
}
