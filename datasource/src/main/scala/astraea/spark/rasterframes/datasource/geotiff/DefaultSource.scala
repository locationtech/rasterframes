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

import java.net.URI

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import astraea.spark.rasterframes._
import astraea.spark.rasterframes.util._
import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.io.geotiff.GeoTiff

import scala.util.Try

/**
 *
 * @since 1/14/18
 */
@Experimental
class DefaultSource extends DataSourceRegister with RelationProvider with CreatableRelationProvider with LazyLogging {
  def shortName() = DefaultSource.SHORT_NAME

  def path(parameters: Map[String, String]) =
    parameters.get(DefaultSource.PATH_PARAM).flatMap(p ⇒ Try(URI.create(p)).toOption)

  def num(key: String, parameters: Map[String, String]) =
    parameters.get(key).map(_.toLong)


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

    val tl = rf.tileLayerMetadata.widen.layout.tileLayout

    val cols = num(DefaultSource.IMAGE_WIDTH_PARAM, parameters).getOrElse(tl.totalCols)
    val rows = num(DefaultSource.IMAGE_HEIGHT_PARAM, parameters).getOrElse(tl.totalRows)

    require(cols <= Int.MaxValue && rows <= Int.MaxValue, s"Can't construct a GeoTIFF of size $cols x $rows. (Too big!)")

    // Should we really play traffic cop here?
    if(cols.toDouble * rows * 64.0 > Runtime.getRuntime.totalMemory() * 0.5)
      logger.warn(s"You've asked for the construction of a very large image ($cols x $rows), destined for ${pathO.get}. Out of memory error likely.")

    println()
    val raster = rf.toMultibandRaster(rf.tileColumns, cols.toInt, rows.toInt)

    GeoTiff(raster).write(pathO.get.getPath)
    GeoTiffRelation(sqlContext, pathO.get)
  }
}

object DefaultSource {
  final val SHORT_NAME = "geotiff"
  final val PATH_PARAM = "path"
  final val IMAGE_WIDTH_PARAM = "imageWidth"
  final val IMAGE_HEIGHT_PARAM = "imageWidth"
}
