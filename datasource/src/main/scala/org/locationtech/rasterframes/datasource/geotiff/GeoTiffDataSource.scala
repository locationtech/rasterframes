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

import _root_.geotrellis.raster.io.geotiff.compression._
import _root_.geotrellis.raster.io.geotiff.tags.codes.ColorSpace
import _root_.geotrellis.raster.io.geotiff.{GeoTiffOptions, MultibandGeoTiff, Tags, Tiled}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.datasource._
import org.locationtech.rasterframes.expressions.aggregates.TileRasterizerAggregate
import org.locationtech.rasterframes.expressions.aggregates.TileRasterizerAggregate.ProjectedRasterDefinition
import org.locationtech.rasterframes.util._

/**
 * Spark SQL data source over GeoTIFF files.
 * @since 1/14/18
 */
class GeoTiffDataSource extends DataSourceRegister
  with RelationProvider with CreatableRelationProvider
  with DataSourceOptions with LazyLogging {
  def shortName() = GeoTiffDataSource.SHORT_NAME

  def path(parameters: Map[String, String]) =
    uriParam(PATH_PARAM, parameters)

  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    val pathO = path(parameters)
    require(pathO.isDefined, "Valid URI 'path' parameter required.")
    sqlContext.withRasterFrames

    val p = pathO.get

    if(p.getPath.contains("*")) {
      val bandCount = parameters.get(GeoTiffDataSource.BAND_COUNT_PARAM).map(_.toInt).getOrElse(1)
      GeoTiffCollectionRelation(sqlContext, p, bandCount)
    }
    else GeoTiffRelation(sqlContext, p)
  }

  def createLayer(df: DataFrame, parameters: Map[String, String], tileCols: Seq[Column]): RasterFrameLayer = {
    require(tileCols.nonEmpty, "need at least one tile column")


//    val prd = ProjectedRasterDefinition()
//    val crsCol = col("crs")
//    val extentCol = col("extent")
//    df.agg(TileRasterizerAggregate(prd, crsCol, extentCol, tileCols.head))
    ???
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val pathO = path(parameters)
    require(pathO.isDefined, "Valid URI 'path' parameter required.")
    require(pathO.get.getScheme == "file" || pathO.get.getScheme == null, "Currently only 'file://' destinations are supported")
    sqlContext.withRasterFrames

    val compress = parameters.get(GeoTiffDataSource.COMPRESS_PARAM).exists(_.toBoolean)
    val tcols = data.tileColumns

    require(tcols.nonEmpty, "Could not find any tile columns.")

    val tags = Tags(
      RFBuildInfo.toMap.filter(_._1.startsWith("rf")).mapValues(_.toString),
      tcols.map(c ⇒ Map("RF_COL" -> c.columnName)).toList
    )

    // We make some assumptions here.... eventually have column metadata encode this.
    val colorSpace = tcols.size match {
      case 3 | 4 ⇒  ColorSpace.RGB
      case _ ⇒ ColorSpace.BlackIsZero
    }

    val tiffOptions = GeoTiffOptions(Tiled, if (compress) DeflateCompression else NoCompression, colorSpace)

    val layer = if(data.isLayer) data.certify else createLayer(data, parameters, tcols)

    val tlm = layer.tileLayerMetadata.merge
    // If no desired image size is given, write at full size.
    val cols = numParam(GeoTiffDataSource.IMAGE_WIDTH_PARAM, parameters).getOrElse(tlm.gridBounds.width.toLong)
    val rows = numParam(GeoTiffDataSource.IMAGE_HEIGHT_PARAM, parameters).getOrElse(tlm.gridBounds.height.toLong)

    require(cols <= Int.MaxValue && rows <= Int.MaxValue, s"Can't construct a GeoTIFF of size $cols x $rows. (Too big!)")

    // Should we really play traffic cop here?
    if(cols.toDouble * rows * 64.0 > Runtime.getRuntime.totalMemory() * 0.5)
      logger.warn(s"You've asked for the construction of a very large image ($cols x $rows), destined for ${pathO.get}. Out of memory error likely.")

    val raster = layer.toMultibandRaster(tcols, cols.toInt, rows.toInt)

    val geotiff = new MultibandGeoTiff(raster.tile, raster.extent, raster.crs, tags, tiffOptions)

    logger.debug(s"Writing DataFrame to GeoTIFF ($cols x $rows) at ${pathO.get}")
    geotiff.write(pathO.get.getPath)
    GeoTiffRelation(sqlContext, pathO.get)
  }
}

object GeoTiffDataSource {
  final val SHORT_NAME = "geotiff"
  final val PATH_PARAM = "path"
  final val IMAGE_WIDTH_PARAM = "imageWidth"
  final val IMAGE_HEIGHT_PARAM = "imageWidth"
  final val COMPRESS_PARAM = "compress"
  final val BAND_COUNT_PARAM = "bandCount"
}
