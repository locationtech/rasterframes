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
 */

package astraea.spark.rasterframes.datasource.geotiff

import astraea.spark.rasterframes._
import astraea.spark.rasterframes.datasource._
import astraea.spark.rasterframes.util._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, functions ⇒ F}
import _root_.geotrellis.raster.io.geotiff.{GeoTiffOptions, MultibandGeoTiff, Tags, Tiled}
import _root_.geotrellis.raster.io.geotiff.compression._
import _root_.geotrellis.raster.io.geotiff.tags.codes.ColorSpace

/**
 * Spark SQL data source over GeoTIFF files.
 * @since 1/14/18
 */
class DefaultSource extends DataSourceRegister with RelationProvider with CreatableRelationProvider with LazyLogging {
  def shortName() = DefaultSource.SHORT_NAME

  def path(parameters: Map[String, String]) =
    uriParam(DefaultSource.PATH_PARAM, parameters)

  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    val pathO = path(parameters)
    require(pathO.isDefined, "Valid URI 'path' parameter required.")
    sqlContext.withRasterFrames
    GeoTiffRelation(sqlContext, pathO.get)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val pathO = path(parameters)
    require(pathO.isDefined, "Valid URI 'path' parameter required.")
    require(pathO.get.getScheme == "file" || pathO.get.getScheme == null, "Currently only 'file://' destinations are supported")
    sqlContext.withRasterFrames

    require(data.isRF, "GeoTIFF can only be constructed from a RasterFrame")
    val rf = data.certify

    // If no desired image size is given, write at full size.
    lazy val (fullResCols, fullResRows) = {
      // get the layout size given that the tiles may be heterogenously sized
      // first get any valid row and column in the spatial key structure
      val sk = rf.select(SPATIAL_KEY_COLUMN).first()

      val tc = rf.tileColumns.head

      val c = rf
        .where(SPATIAL_KEY_COLUMN("row") === sk.row)
        .agg(
          F.sum(tileDimensions(tc)("cols") cast(LongType))
        ).first()
        .getLong(0)

      val r = rf
        .where(SPATIAL_KEY_COLUMN("col") === sk.col)
        .agg(
          F.sum(tileDimensions(tc)("rows") cast(LongType))
        ).first()
        .getLong(0)

      (c, r)
    }

    val cols = numParam(DefaultSource.IMAGE_WIDTH_PARAM, parameters).getOrElse(fullResCols)
    val rows = numParam(DefaultSource.IMAGE_HEIGHT_PARAM, parameters).getOrElse(fullResRows)

    require(cols <= Int.MaxValue && rows <= Int.MaxValue, s"Can't construct a GeoTIFF of size $cols x $rows. (Too big!)")

    // Should we really play traffic cop here?
    if(cols.toDouble * rows * 64.0 > Runtime.getRuntime.totalMemory() * 0.5)
      logger.warn(s"You've asked for the construction of a very large image ($cols x $rows), destined for ${pathO.get}. Out of memory error likely.")

    val tcols = rf.tileColumns
    val raster = rf.toMultibandRaster(tcols, cols.toInt, rows.toInt)

    // We make some assumptions here.... eventually have column metadata encode this.
    val colorSpace = tcols.size match {
      case 3 | 4 ⇒  ColorSpace.RGB
      case _ ⇒ ColorSpace.BlackIsZero
    }

    val compress = parameters.get(DefaultSource.COMPRESS).map(_.toBoolean).getOrElse(false)
    val options = GeoTiffOptions(Tiled, if (compress) DeflateCompression else NoCompression, colorSpace)
    val tags = Tags(
      RFBuildInfo.toMap.filter(_._1.startsWith("rf")).mapValues(_.toString),
      tcols.map(c ⇒ Map("RF_COL" -> c.columnName)).toList
    )
    val geotiff = new MultibandGeoTiff(raster.tile, raster.extent, raster.crs, tags, options)

    logger.debug(s"Writing DataFrame to GeoTIFF ($cols x $rows) at ${pathO.get}")
    geotiff.write(pathO.get.getPath)
    GeoTiffRelation(sqlContext, pathO.get)
  }
}

object DefaultSource {
  final val SHORT_NAME = "geotiff"
  final val PATH_PARAM = "path"
  final val IMAGE_WIDTH_PARAM = "imageWidth"
  final val IMAGE_HEIGHT_PARAM = "imageWidth"
  final val COMPRESS = "compress"
}
