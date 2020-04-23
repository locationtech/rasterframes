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
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.locationtech.rasterframes.extensions

import geotrellis.layer._
import geotrellis.raster.CellGrid
import geotrellis.util.MethodExtensions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.locationtech.rasterframes.PairRDDConverter._
import org.locationtech.rasterframes.{PairRDDConverter, RasterFrameLayer}
import org.locationtech.rasterframes.StandardColumns._
import org.locationtech.rasterframes.extensions.Implicits._
import org.locationtech.rasterframes.util.JsonCodecs._
import org.locationtech.rasterframes.util._

/**
 * Extension method on `ContextRDD`-shaped RDDs with appropriate context bounds to create a RasterFrameLayer.
 * @since 7/18/17
 */
abstract class SpatialContextRDDMethods[T <: CellGrid[Int]](implicit spark: SparkSession)
    extends MethodExtensions[RDD[(SpatialKey, T)] with Metadata[TileLayerMetadata[SpatialKey]]] {
  import PairRDDConverter._

  def toLayer(implicit converter: PairRDDConverter[SpatialKey, T]): RasterFrameLayer = toLayer(TILE_COLUMN.columnName)

  def toLayer(tileColumnName: String)(implicit converter: PairRDDConverter[SpatialKey, T]): RasterFrameLayer = {
    val df = self.toDataFrame.setSpatialColumnRole(SPATIAL_KEY_COLUMN, self.metadata)
    val defName = TILE_COLUMN.columnName
    df.applyWhen(_ ⇒ tileColumnName != defName, _.withColumnRenamed(defName, tileColumnName))
      .certify
  }
}

/**
 * Extension method on `ContextRDD`-shaped `Tile` RDDs keyed with [[SpaceTimeKey]], with appropriate context bounds to create a RasterFrameLayer.
 * @since 9/11/17
 */
abstract class SpatioTemporalContextRDDMethods[T <: CellGrid[Int]](
  implicit spark: SparkSession)
  extends MethodExtensions[RDD[(SpaceTimeKey, T)] with Metadata[TileLayerMetadata[SpaceTimeKey]]] {

  def toLayer(implicit converter: PairRDDConverter[SpaceTimeKey, T]): RasterFrameLayer = toLayer(TILE_COLUMN.columnName)

  def toLayer(tileColumnName: String)(implicit converter: PairRDDConverter[SpaceTimeKey, T]): RasterFrameLayer = {
    val df = self.toDataFrame
      .setSpatialColumnRole(SPATIAL_KEY_COLUMN, self.metadata)
      .setTemporalColumnRole(TEMPORAL_KEY_COLUMN)
    val defName = TILE_COLUMN.columnName
    df.applyWhen(_ ⇒ tileColumnName != defName, _.withColumnRenamed(defName, tileColumnName))
      .certify
  }
}
