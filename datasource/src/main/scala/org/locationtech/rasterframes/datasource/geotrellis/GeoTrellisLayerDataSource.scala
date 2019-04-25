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

package org.locationtech.rasterframes.datasource.geotrellis

import java.net.URI

import org.locationtech.rasterframes._
import org.locationtech.rasterframes.datasource.DataSourceOptions
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql._
import org.apache.spark.sql.sources._

import scala.util.Try


/**
 * DataSource over a GeoTrellis layer store.
 */
@Experimental
class GeoTrellisLayerDataSource extends DataSourceRegister
  with RelationProvider with CreatableRelationProvider with DataSourceOptions {
  def shortName(): String = GeoTrellisLayerDataSource.SHORT_NAME

  /**
   * Create a GeoTrellis data source.
   * @param sqlContext spark stuff
   * @param parameters required parameters are:
   *                   `path`-layer store URI (e.g. "s3://bucket/gt_layers;
   *                   `layer`-layer name (e.g. "LC08_L1GT");
   *                   `zoom`-positive integer zoom level (e.g. "8");
   *                   `numPartitions`-(optional) integer specifying initial number of partitions;
   *                   `tileSubdivisions`-(optional) positive integer defining how many division horizontally and vertically should be applied to a tile;
   *                   `failOnUnrecognizedFilter`-(optional) if true, predicate push-down filters not translated into GeoTrellis query syntax are fatal.
   */
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    require(parameters.contains(PATH_PARAM), s"'$PATH_PARAM' parameter required.")
    require(parameters.contains(LAYER_PARAM), s"'$LAYER_PARAM' parameter for raster layer name required.")
    require(parameters.contains(ZOOM_PARAM), s"'$ZOOM_PARAM' parameter for raster layer zoom level required.")

    sqlContext.withRasterFrames

    val uri: URI = URI.create(parameters(PATH_PARAM))
    val layerId: LayerId = LayerId(parameters(LAYER_PARAM), parameters(ZOOM_PARAM).toInt)
    val numPartitions = parameters.get(NUM_PARTITIONS_PARAM).map(_.toInt)
    val tileSubdivisions = parameters.get(TILE_SUBDIVISIONS_PARAM).map(_.toInt)
    tileSubdivisions.foreach(s ⇒ require(s >= 0, TILE_SUBDIVISIONS_PARAM + " must be a postive integer"))
    val failOnUnrecognizedFilter = parameters.get("failOnUnrecognizedFilter").exists(_.toBoolean)

    GeoTrellisRelation(sqlContext, uri, layerId, numPartitions, failOnUnrecognizedFilter, tileSubdivisions)
  }

  /** Write relation. */
  def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val zoom = parameters.get(ZOOM_PARAM).flatMap(p ⇒ Try(p.toInt).toOption)
    val path = parameters.get(PATH_PARAM).flatMap(p ⇒ Try(new URI(p)).toOption)
    val layerName = parameters.get(LAYER_PARAM)

    require(path.isDefined, s"Valid URI '$PATH_PARAM' parameter required.")
    require(layerName.isDefined, s"'$LAYER_PARAM' parameter for raster layer name required.")
    require(zoom.isDefined, s"Integer '$ZOOM_PARAM' parameter for raster layer zoom level required.")

    val rf = data.asRFSafely
      .getOrElse(throw new IllegalArgumentException("Only a valid RasterFrame can be saved as a GeoTrellis layer"))

    val tileColumn = parameters.get(TILE_COLUMN_PARAM).map(c ⇒ rf(c))

    val layerId = for {
      name ← layerName
      z ← zoom
    } yield LayerId(name, z)

    lazy val writer = LayerWriter(path.get)

    if(tileColumn.isDefined || rf.tileColumns.length == 1) {
      val tileCol: Column = tileColumn.getOrElse(rf.tileColumns.head)
      val eitherRDD = rf.toTileLayerRDD(tileCol)
      eitherRDD.fold(
        skLayer ⇒ writer.write(layerId.get, skLayer, ZCurveKeyIndexMethod),
        stkLayer ⇒ writer.write(layerId.get, stkLayer, ZCurveKeyIndexMethod.byDay())
      )
    }
    else {
      rf.toMultibandTileLayerRDD.fold(
        skLayer ⇒ writer.write(layerId.get, skLayer, ZCurveKeyIndexMethod),
        stkLayer ⇒ writer.write(layerId.get, stkLayer, ZCurveKeyIndexMethod.byDay())
      )
    }

    createRelation(sqlContext, parameters)
  }
}

object GeoTrellisLayerDataSource {
  final val SHORT_NAME = "geotrellis"
}
