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

import astraea.spark.rasterframes.RasterFrame
import astraea.spark.rasterframes.extensions.Implicits._
import astraea.spark.rasterframes.StandardColumns._
import geotrellis.raster.{Tile, TileFeature}
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.util.MethodExtensions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import spray.json.JsonFormat
import astraea.spark.rasterframes.util._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._


/**
 * Extension method on `ContextRDD`-shaped [[Tile]] RDDs with appropriate context bounds to create a RasterFrame.
 * @since 7/18/17
 */
abstract class SpatialContextRDDMethods(implicit spark: SparkSession)
    extends MethodExtensions[RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]] {

  def toRF: RasterFrame = toRF(TILE_COLUMN.columnName)

  def toRF(tileColumnName: String): RasterFrame = {
    import spark.implicits._

    val rdd = self: RDD[(SpatialKey, Tile)]
    val df = rdd
      .toDF(SPATIAL_KEY_COLUMN.columnName, tileColumnName)

    df.setSpatialColumnRole(SPATIAL_KEY_COLUMN, self.metadata)
      .certify
  }
}

/**
 * Extension method on `ContextRDD`-shaped [[Tile]] RDDs keyed with [[SpaceTimeKey]], with appropriate context bounds to create a RasterFrame.
 * @since 9/11/17
 */
abstract class SpatioTemporalContextRDDMethods(implicit spark: SparkSession)
  extends MethodExtensions[RDD[(SpaceTimeKey, Tile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]] {

  def toRF: RasterFrame = {
    import spark.implicits._

    val rdd = self: RDD[(SpaceTimeKey, Tile)]
    val df = rdd
      .map { case (k, v) ⇒ (k.spatialKey, k.temporalKey, v)}
      .toDF(SPATIAL_KEY_COLUMN.columnName, TEMPORAL_KEY_COLUMN.columnName, TILE_COLUMN.columnName)

    df
      .setSpatialColumnRole(SPATIAL_KEY_COLUMN, self.metadata)
      .setTemporalColumnRole(TEMPORAL_KEY_COLUMN)
      .certify
  }
}

/**
 * Extension method on `ContextRDD`-shaped [[TileFeature]] RDDs with appropriate context bounds to create a RasterFrame.
 * @since 7/18/17
 */
abstract class TFContextRDDMethods[K: SpatialComponent: JsonFormat: ClassTag: TypeTag, D: TypeTag](implicit spark: SparkSession)
    extends MethodExtensions[RDD[(K, TileFeature[Tile, D])] with Metadata[TileLayerMetadata[K]]] {

  def toRF: RasterFrame = {
    import spark.implicits._
    val rdd = self: RDD[(K, TileFeature[Tile, D])]

    val df = rdd
      .map { case (k, v) ⇒ (k, v.tile, v.data) }
      .toDF(SPATIAL_KEY_COLUMN.columnName, TILE_COLUMN.columnName, TILE_FEATURE_DATA_COLUMN.columnName)

    df
      .setSpatialColumnRole(SPATIAL_KEY_COLUMN, self.metadata)
      .certify
  }
}

/**
 * Extension method on `ContextRDD`-shaped [[TileFeature]] RDDs with appropriate context bounds to create a RasterFrame.
 * @since 7/18/17
 */
abstract class TFSTContextRDDMethods[D: TypeTag](implicit spark: SparkSession)
  extends MethodExtensions[RDD[(SpaceTimeKey, TileFeature[Tile, D])] with Metadata[TileLayerMetadata[SpaceTimeKey]]] {

  def toRF: RasterFrame = {
    import spark.implicits._
    val rdd = self: RDD[(SpaceTimeKey, TileFeature[Tile, D])]

    val df = rdd
      .map { case (k, v) ⇒ (k.spatialKey, k.temporalKey, v.tile, v.data)}
      .toDF(SPATIAL_KEY_COLUMN.columnName, TEMPORAL_KEY_COLUMN.columnName, TILE_COLUMN.columnName, TILE_FEATURE_DATA_COLUMN.columnName)

    df
      .setSpatialColumnRole(SPATIAL_KEY_COLUMN, self.metadata)
      .setTemporalColumnRole(TEMPORAL_KEY_COLUMN)
      .certify
  }
}
