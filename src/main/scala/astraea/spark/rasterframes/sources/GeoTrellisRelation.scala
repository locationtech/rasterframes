/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Astraea, Inc.
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

package astraea.spark.rasterframes.sources

import java.net.URI

import geotrellis.raster.Tile
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.file.FileLayerReader
import geotrellis.spark.io.hadoop.HadoopLayerReader
import geotrellis.util.LazyLogging
import geotrellis.vector.Extent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.gt.types._
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

case class GeoTrellisRelation(sqlContext: SQLContext, uri: URI, layerId: LayerId, bbox: Option[Extent])
    extends BaseRelation
    with PrunedFilteredScan
    with LazyLogging {

  // TODO: implement sizeInBytes

  override def schema: StructType =
    StructType(
      List(
        StructField("col", DataTypes.IntegerType, nullable = false),
        StructField("row", DataTypes.IntegerType, nullable = false),
        StructField(
          "extent",
          StructType(
            List(
              StructField("xmin", DataTypes.DoubleType, nullable = false),
              StructField("xmax", DataTypes.DoubleType, nullable = false),
              StructField("ymin", DataTypes.DoubleType, nullable = false),
              StructField("ymax", DataTypes.DoubleType, nullable = false)
            )
          )
        ),
        StructField("tile", TileUDT, nullable = true)
      )
    )

  def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    buildScan(requiredColumns, Array.empty[Filter])
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    // only primitive comparisons can be pushed down, not UDFs
    // @see DataSourceStrategy.scala:509 translateFilter()
    filters
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    logger.debug(s"Reading: $layerId from $uri")
    logger.debug(s"Required columns: ${requiredColumns.toList}")
    logger.debug(s"PushedDown filters: ${filters.toList}")

    implicit val sc = sqlContext.sparkContext

    // TODO: check they type of layer before reading, generating time column dynamically
    // FIX: do not ignore requiredColumns, it breaks DataFrames selection from DataSource
    val reader = GeoTrellisRelation.layerReaderFromUri(uri)
    val query = reader.query[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
    val rdd = bbox match {
      case Some(extent) ⇒ query.where(Intersects(extent)).result
      case None ⇒ query.result
    }

    rdd.map {
      case (sk: SpatialKey, tile: Tile) ⇒
        val key_extent: Extent = rdd.metadata.layout.mapTransform(sk)
        Row(sk.col, sk.row, key_extent, tile)
    }
  }
}

object GeoTrellisRelation {
  def layerReaderFromUri(uri: URI)(implicit sc: SparkContext): FilteringLayerReader[LayerId] = {
    uri.getScheme match {
      case "file" ⇒
        FileLayerReader(uri.getSchemeSpecificPart)

      case "hdfs" ⇒
        val path = new org.apache.hadoop.fs.Path(uri)
        HadoopLayerReader(path)

      // others require modules outside of geotrellis-spark
    }
  }
}
