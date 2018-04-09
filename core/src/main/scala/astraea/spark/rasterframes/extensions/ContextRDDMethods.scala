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

import astraea.spark.rasterframes.PairRDDConverter._
import astraea.spark.rasterframes.StandardColumns._
import astraea.spark.rasterframes.extensions.Implicits._
import astraea.spark.rasterframes.util._
import astraea.spark.rasterframes.{PairRDDConverter, RasterFrame}
import geotrellis.raster.{CellGrid, Tile}
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.util.MethodExtensions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Extension method on `ContextRDD`-shaped RDDs with appropriate context bounds to create a RasterFrame.
 * @since 7/18/17
 */
abstract class SpatialContextRDDMethods[T <: CellGrid](implicit spark: SparkSession)
    extends MethodExtensions[RDD[(SpatialKey, T)] with Metadata[TileLayerMetadata[SpatialKey]]] {
  import PairRDDConverter._

  def toRF(implicit converter: PairRDDConverter[SpatialKey, T]): RasterFrame = toRF(TILE_COLUMN.columnName)

  def toRF(tileColumnName: String)(implicit converter: PairRDDConverter[SpatialKey, T]): RasterFrame = {
    val df = self.toDataFrame
    df.setSpatialColumnRole(SPATIAL_KEY_COLUMN, self.metadata)
      .certify
  }
}

/**
 * Extension method on `ContextRDD`-shaped [[Tile]] RDDs keyed with [[SpaceTimeKey]], with appropriate context bounds to create a RasterFrame.
 * @since 9/11/17
 */
abstract class SpatioTemporalContextRDDMethods[T <: CellGrid](
  implicit spark: SparkSession)
  extends MethodExtensions[RDD[(SpaceTimeKey, T)] with Metadata[TileLayerMetadata[SpaceTimeKey]]] {

  def toRF(implicit converter: PairRDDConverter[SpaceTimeKey, T]): RasterFrame = toRF(TILE_COLUMN.columnName)

  def toRF(tileColumnName: String)(implicit converter: PairRDDConverter[SpaceTimeKey, T]): RasterFrame = {
    self.toDataFrame
      .setSpatialColumnRole(SPATIAL_KEY_COLUMN, self.metadata)
      .setTemporalColumnRole(TEMPORAL_KEY_COLUMN)
      .certify
  }
}
