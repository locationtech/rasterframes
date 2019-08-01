/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea, Inc.
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

package org.locationtech.rasterframes.datasource.geotiff

import java.net.URI

import _root_.geotrellis.proj4.CRS
import _root_.geotrellis.raster._
import _root_.geotrellis.raster.io.geotiff.compression._
import _root_.geotrellis.raster.io.geotiff.tags.codes.ColorSpace
import _root_.geotrellis.raster.io.geotiff.{GeoTiffOptions, MultibandGeoTiff, Tags, Tiled}
<<<<<<< HEAD
=======
import _root_.geotrellis.spark._
>>>>>>> develop
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.datasource._
import org.locationtech.rasterframes.expressions.aggregates.TileRasterizerAggregate.ProjectedRasterDefinition
import org.locationtech.rasterframes.expressions.aggregates.{ProjectedLayerMetadataAggregate, TileRasterizerAggregate}
import org.locationtech.rasterframes.model.{LazyCRS, TileDimensions}
import org.locationtech.rasterframes.util._

/**
 * Spark SQL data source over GeoTIFF files.
 * @since 1/14/18
 */
class GeoTiffDataSource extends DataSourceRegister
  with RelationProvider with CreatableRelationProvider
  with DataSourceOptions with LazyLogging {
  import GeoTiffDataSource._

  def shortName() = GeoTiffDataSource.SHORT_NAME

  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    require(parameters.path.isDefined, "Valid URI 'path' parameter required.")
    sqlContext.withRasterFrames

    val p = parameters.path.get

    if(p.getPath.contains("*")) {
      val bandCount = parameters.get(GeoTiffDataSource.BAND_COUNT_PARAM).map(_.toInt).getOrElse(1)
      GeoTiffCollectionRelation(sqlContext, p, bandCount)
    }
    else GeoTiffRelation(sqlContext, p)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], df: DataFrame): BaseRelation = {
    require(parameters.path.isDefined, "Valid URI 'path' parameter required.")
    val path = parameters.path.get
    require(path.getScheme == "file" || path.getScheme == null,
      "Currently only 'file://' destinations are supported")
    sqlContext.withRasterFrames

    val tileCols = df.tileColumns

    require(tileCols.nonEmpty, "Could not find any tile columns.")

    val raster = if(df.isAlreadyLayer) {
      val layer = df.certify
      val tlm = layer.tileLayerMetadata.merge

      // If no desired image size is given, write at full size.
      val TileDimensions(cols, rows) =  parameters.rasterDimensions
        .getOrElse {
          val actualSize = tlm.layout.toRasterExtent().gridBoundsFor(tlm.extent)
          TileDimensions(actualSize.width, actualSize.height)
        }

      // Should we really play traffic cop here?
      if(cols.toDouble * rows * 64.0 > Runtime.getRuntime.totalMemory() * 0.5)
        logger.warn(s"You've asked for the construction of a very large image ($cols x $rows), destined for ${path}. Out of memory error likely.")

      layer.toMultibandRaster(tileCols, cols.toInt, rows.toInt)
    }
    else {
      require(parameters.crs.nonEmpty, "A destination CRS must be provided")
      require(tileCols.nonEmpty, "need at least one tile column")

      // Grab CRS to project into
      val destCRS = parameters.crs.get

      // Select the anchoring Tile, Extent and CRS columns
      val (extCol, crsCol, tileCol) = {
        // Favor "ProjectedRaster" columns
        val prCols = df.projRasterColumns
        if(prCols.nonEmpty) {
          (rf_extent(prCols.head), rf_crs(prCols.head), rf_tile(prCols.head))
        }
        else {
          // If no "ProjectedRaster" column, look for single Extent and CRS columns.
          val crsCols = df.crsColumns
          require(crsCols.size == 1, "Exactly one CRS column must be in DataFrame")
          val extentCols = df.extentColumns
          require(extentCols.size == 1, "Exactly one Extent column must be in DataFrame")
          (extentCols.head, crsCols.head, tileCols.head)
        }
      }

      // Scan table and constuct what the TileLayerMetadata would be in the specified destination CRS.
      val tlm: TileLayerMetadata[SpatialKey] = df
        .select(ProjectedLayerMetadataAggregate(
          destCRS, extCol, crsCol, rf_cell_type(tileCol), rf_dimensions(tileCol)
        ))
        .first()

      val c = ProjectedRasterDefinition(tlm)

      val config = parameters.rasterDimensions.map { dims =>
        c.copy(totalCols = dims.cols, totalRows = dims.rows)
      }.getOrElse(c)

      val aggs = tileCols
        .map(t => TileRasterizerAggregate(
          config, crsCol, extCol, rf_tile(t))("tile").as(t.columnName)
        )

      val agg = df.select(aggs: _*)

      val row = agg.first()

      val bands = for(i <- 0 until row.size) yield row.getAs[Tile](i)

      ProjectedRaster(MultibandTile(bands), tlm.extent, tlm.crs)
    }

    val tags = Tags(
      RFBuildInfo.toMap.filter(_._1.startsWith("rf")).mapValues(_.toString),
      tileCols.map(c ⇒ Map("RF_COL" -> c.columnName)).toList
    )

    // We make some assumptions here.... eventually have column metadata encode this.
    val colorSpace = tileCols.size match {
      case 3 | 4 ⇒  ColorSpace.RGB
      case _ ⇒ ColorSpace.BlackIsZero
    }

    val tiffOptions = GeoTiffOptions(Tiled,
      if (parameters.compress) DeflateCompression else NoCompression, colorSpace
    )

    val geotiff = new MultibandGeoTiff(raster.tile, raster.extent, raster.crs, tags, tiffOptions)

    logger.debug(s"Writing DataFrame to GeoTIFF (${geotiff.cols} x ${geotiff.rows}) at ${path}")
    geotiff.write(path.getPath)
    GeoTiffRelation(sqlContext, path)
  }
}

object GeoTiffDataSource {
  final val SHORT_NAME = "geotiff"
  final val PATH_PARAM = "path"
  final val IMAGE_WIDTH_PARAM = "imageWidth"
  final val IMAGE_HEIGHT_PARAM = "imageHeight"
  final val COMPRESS_PARAM = "compress"
  final val CRS_PARAM = "crs"
  final val BAND_COUNT_PARAM = "bandCount"

  private[geotiff]
  implicit class ParamsDictAccessors(val parameters: Map[String, String]) extends AnyVal {
    def path: Option[URI] = uriParam(PATH_PARAM, parameters)
    def compress: Boolean = parameters.get(COMPRESS_PARAM).exists(_.toBoolean)
    def crs: Option[CRS] = parameters.get(CRS_PARAM).map(s => LazyCRS(s))
    def rasterDimensions: Option[TileDimensions] = {
      numParam(IMAGE_WIDTH_PARAM, parameters)
        .zip(numParam(IMAGE_HEIGHT_PARAM, parameters))
        .map { case (cols, rows) =>
          require(cols <= Int.MaxValue && rows <= Int.MaxValue,
            s"Can't construct a GeoTIFF of size $cols x $rows. (Too big!)")
          TileDimensions(cols.toInt, rows.toInt)
        }.headOption
    }
  }
}
