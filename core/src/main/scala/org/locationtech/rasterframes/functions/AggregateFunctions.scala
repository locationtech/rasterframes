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
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster.{IntConstantNoDataCellType, Raster, Tile}
import geotrellis.vector.Extent
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.rasterframes.expressions.accessors.{ExtractTile, GetCRS, GetExtent}
import org.locationtech.rasterframes.expressions.aggregates.TileRasterizerAggregate.ProjectedRasterDefinition
import org.locationtech.rasterframes.expressions.aggregates._
import org.locationtech.rasterframes.stats._

/** Functions associated with computing columnar aggregates over tile and geometry columns. */
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

  /** Construct an overview raster of size `cols`x`rows` where data in `proj_raster` intersects the
    * `areaOfExtent` in web-mercator. */
  def rf_agg_overview_raster(cols: Int, rows: Int, areaOfInterest: Extent, proj_raster: Column): TypedColumn[Any, Raster[Tile]] =
    rf_agg_overview_raster(cols, rows, areaOfInterest, GetExtent(proj_raster), GetCRS(proj_raster), ExtractTile(proj_raster))

  /** Construct an overview raster of size `cols`x`rows` where data in `tile` intersects the `areaOfExtent` in web-mercator. */
  def rf_agg_overview_raster(cols: Int, rows: Int, areaOfInterest: Extent, tileExtent: Column, tileCRS: Column, tile: Column): TypedColumn[Any, Raster[Tile]] = {
    val params = ProjectedRasterDefinition(cols, rows, IntConstantNoDataCellType, WebMercator, areaOfInterest)
    TileRasterizerAggregate(params, tileCRS, tileExtent, tile)
  }

  /** Compute the aggregate extent over a column. */
  def rf_agg_extent(extent: Column) = {
    import org.locationtech.rasterframes.encoders.StandardEncoders.extentEncoder
    import org.apache.spark.sql.functions._
    import org.locationtech.rasterframes.util.NamedColumn
    struct(
      min(extent.getField("xmin")) as "xmin",
      min(extent.getField("ymin")) as "ymin",
      max(extent.getField("xmax")) as "xmax",
      max(extent.getField("ymax")) as "ymax"
    ).as (s"rf_agg_extent(${extent.columnName})").as[Extent]
  }
}

object AggregateFunctions extends AggregateFunctions
