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

package org.locationtech.rasterframes.functions
import geotrellis.raster.Tile
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.rasterframes.expressions.aggregates._
import org.locationtech.rasterframes.stats._

/** Functions associated with computing columnar aggregates over tile columns. */
trait AggregateFunctions {
  /** Compute cell-local aggregate descriptive statistics for a column of Tiles. */
  def rf_agg_local_stats(tile: Column) = LocalStatsAggregate(tile)

  /** Compute the cell-wise/local max operation between Tiles in a column. */
  def rf_agg_local_max(tile: Column): TypedColumn[Any, Tile] = LocalTileOpAggregate.LocalMaxUDAF(tile)

  /** Compute the cellwise/local min operation between Tiles in a column. */
  def rf_agg_local_min(tile: Column): TypedColumn[Any, Tile] = LocalTileOpAggregate.LocalMinUDAF(tile)

  /** Compute the cellwise/local mean operation between Tiles in a column. */
  def rf_agg_local_mean(tile: Column): TypedColumn[Any, Tile] = LocalMeanAggregate(tile)

  /** Compute the cellwise/local count of non-NoData cells for all Tiles in a column. */
  def rf_agg_local_data_cells(tile: Column): TypedColumn[Any, Tile] = LocalCountAggregate.LocalDataCellsUDAF(tile)

  /** Compute the cellwise/local count of NoData cells for all Tiles in a column. */
  def rf_agg_local_no_data_cells(tile: Column): TypedColumn[Any, Tile] = LocalCountAggregate.LocalNoDataCellsUDAF(tile)

  /**  Compute the full column aggregate floating point histogram. */
  def rf_agg_approx_histogram(tile: Column): TypedColumn[Any, CellHistogram] = HistogramAggregate(tile)

  /** Compute the full column aggregate floating point statistics. */
  def rf_agg_stats(tile: Column): TypedColumn[Any, CellStatistics] = CellStatsAggregate(tile)

  /** Computes the column aggregate mean. */
  def rf_agg_mean(tile: Column) = CellMeanAggregate(tile)

  /** Computes the number of non-NoData cells in a column. */
  def rf_agg_data_cells(tile: Column): TypedColumn[Any, Long] = CellCountAggregate.DataCells(tile)

  /** Computes the number of NoData cells in a column. */
  def rf_agg_no_data_cells(tile: Column): TypedColumn[Any, Long] = CellCountAggregate.NoDataCells(tile)

  def rf_agg_overview_raster(cols: Int, rows: Int, tiles: Column*): Column = {

    ???
  }

}
