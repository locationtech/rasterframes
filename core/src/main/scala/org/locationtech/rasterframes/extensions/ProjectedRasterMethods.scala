/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2019 Astraea, Inc.
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

import java.time.ZonedDateTime

import org.locationtech.rasterframes.util._
import org.locationtech.rasterframes.{RasterFrame, StandardColumns}
import geotrellis.raster.{CellGrid, ProjectedRaster}
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.util.MethodExtensions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.locationtech.rasterframes.{PairRDDConverter, StandardColumns}

import scala.reflect.runtime.universe._

/**
 * Extension methods on [[ProjectedRaster]] for creating [[RasterFrame]]s.
 *
 * @since 8/10/17
 */
abstract class ProjectedRasterMethods[T <: CellGrid: WithMergeMethods: WithPrototypeMethods: TypeTag]
  extends MethodExtensions[ProjectedRaster[T]] with StandardColumns {
  import Implicits.{WithSpatialContextRDDMethods, WithSpatioTemporalContextRDDMethods}
  type XTileLayerRDD[K] = RDD[(K, T)] with Metadata[TileLayerMetadata[K]]

  /**
   * Convert the wrapped [[ProjectedRaster]] into a [[RasterFrame]] with a
   * single row.
   *
   * @param spark [[SparkSession]] in which to create [[RasterFrame]]
   */
  def toRF(implicit spark: SparkSession, schema: PairRDDConverter[SpatialKey, T]): RasterFrame = toRF(TILE_COLUMN.columnName)

  /**
   * Convert the wrapped [[ProjectedRaster]] into a [[RasterFrame]] with a
   * single row.
   *
   * @param spark [[SparkSession]] in which to create [[RasterFrame]]
   */
  def toRF(tileColName: String)
    (implicit spark: SparkSession, schema: PairRDDConverter[SpatialKey, T]): RasterFrame = {
    val (cols, rows) = self.raster.dimensions
    toRF(cols, rows, tileColName)
  }

  /**
   * Convert the [[ProjectedRaster]] into a [[RasterFrame]] using the
   * given dimensions as the target per-row tile size.
   *
   * @param tileCols Max number of horizontal cells per tile
   * @param tileRows Max number of vertical cells per tile
   * @param spark [[SparkSession]] in which to create [[RasterFrame]]
   */
  def toRF(tileCols: Int, tileRows: Int)
    (implicit spark: SparkSession, schema: PairRDDConverter[SpatialKey, T]): RasterFrame =
    toRF(tileCols, tileRows, TILE_COLUMN.columnName)

  /**
   * Convert the [[ProjectedRaster]] into a [[RasterFrame]] using the
   * given dimensions as the target per-row tile size.
   *
   * @param tileCols Max number of horizontal cells per tile
   * @param tileRows Max number of vertical cells per tile
   * @param tileColName Name to give the created tile column
   * @param spark [[SparkSession]] in which to create [[RasterFrame]]
   */
  def toRF(tileCols: Int, tileRows: Int, tileColName: String)
    (implicit spark: SparkSession, schema: PairRDDConverter[SpatialKey, T]): RasterFrame = {
    toTileLayerRDD(tileCols, tileRows).toRF(tileColName)
  }

  /**
   * Convert the [[ProjectedRaster]] into a [[RasterFrame]] using the
   * given dimensions as the target per-row tile size and singular timestamp as the temporal component.
   *
   * @param tileCols Max number of horizontal cells per tile
   * @param tileRows Max number of vertical cells per tile.
   * @param timestamp Temporal key value to assign to tiles.
   * @param spark [[SparkSession]] in which to create [[RasterFrame]]
   */
  def toRF(tileCols: Int, tileRows: Int, timestamp: ZonedDateTime)
    (implicit spark: SparkSession, schema: PairRDDConverter[SpaceTimeKey, T]): RasterFrame =
    toTileLayerRDD(tileCols, tileRows, timestamp).toRF

  /**
   * Convert the [[ProjectedRaster]] into a [[TileLayerRDD[SpatialKey]] using the
   * given dimensions as the target per-row tile size.
   *
   * @param tileCols Max number of horizontal cells per tile
   * @param tileRows Max number of vertical cells per tile.
   * @param spark [[SparkSession]] in which to create RDD
   */
  def toTileLayerRDD(tileCols: Int,
                     tileRows: Int)(implicit spark: SparkSession): XTileLayerRDD[SpatialKey] = {
    val layout = LayoutDefinition(self.raster.rasterExtent, tileCols, tileRows)
    val kb = KeyBounds(SpatialKey(0, 0), SpatialKey(layout.layoutCols - 1, layout.layoutRows - 1))
    val tlm = TileLayerMetadata(self.tile.cellType, layout, self.extent, self.crs, kb)

    val rdd = spark.sparkContext.makeRDD(Seq((self.projectedExtent, Shims.toArrayTile(self.tile))))

    implicit val tct = typeTag[T].asClassTag

    val tiled = rdd.tileToLayout(tlm)

    ContextRDD(tiled, tlm)
  }

  /**
   * Convert the [[ProjectedRaster]] into a [[TileLayerRDD[SpaceTimeKey]] using the
   * given dimensions as the target per-row tile size and singular timestamp as the temporal component.
   *
   * @param tileCols Max number of horizontal cells per tile
   * @param tileRows Max number of vertical cells per tile.
   * @param timestamp Temporal key value to assign to tiles.
   * @param spark [[SparkSession]] in which to create RDD
   */
  def toTileLayerRDD(tileCols: Int, tileRows: Int, timestamp: ZonedDateTime)(implicit spark: SparkSession): XTileLayerRDD[SpaceTimeKey] = {
    val layout = LayoutDefinition(self.raster.rasterExtent, tileCols, tileRows)
    val kb = KeyBounds(SpaceTimeKey(0, 0, timestamp), SpaceTimeKey(layout.layoutCols - 1, layout.layoutRows - 1, timestamp))
    val tlm = TileLayerMetadata(self.tile.cellType, layout, self.extent, self.crs, kb)

    val rdd = spark.sparkContext.makeRDD(Seq((TemporalProjectedExtent(self.projectedExtent, timestamp), self.tile)))

    implicit val tct = typeTag[T].asClassTag

    val tiled = rdd.tileToLayout(tlm)

    ContextRDD(tiled, tlm)
  }
}
