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
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster.resample.ResampleMethod
import geotrellis.raster.{IntConstantNoDataCellType, Tile}
import geotrellis.vector.Extent
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.rasterframes.expressions.accessors.{ExtractTile, GetCRS, GetExtent}
import org.locationtech.rasterframes.expressions.aggregates.TileRasterizerAggregate.ProjectedRasterDefinition
import org.locationtech.rasterframes.expressions.aggregates._
import org.locationtech.rasterframes.stats._
import org.locationtech.rasterframes._

/** Functions associated with computing columnar aggregates over tile and geometry columns. */
trait AggregateFunctions {
  /** Compute cell-local aggregate descriptive statistics for a column of Tiles. */
  def rf_agg_local_stats(tile: Column): TypedColumn[Any, LocalCellStatistics] = LocalStatsAggregate(tile)

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

  /**  Compute the approximate aggregate floating point histogram using a streaming algorithm, with the default of 80 buckets. */
  def rf_agg_approx_histogram(tile: Column): TypedColumn[Any, CellHistogram] = HistogramAggregate(tile)

  /**  Compute the approximate aggregate floating point histogram using a streaming algorithm, with the given number of buckets. */
  def rf_agg_approx_histogram(col: Column, numBuckets: Int): TypedColumn[Any, CellHistogram] = {
    require(numBuckets > 0, "Must provide a positive number of buckets")
    HistogramAggregate(col, numBuckets)
  }

  /**
    * Calculates the approximate quantiles of a tile column of a DataFrame.
    * @param tile tile column to extract cells from.
    * @param probabilities a list of quantile probabilities
    *   Each number must belong to [0, 1].
    *   For example 0 is the minimum, 0.5 is the median, 1 is the maximum.
    * @param relativeError The relative target precision to achieve (greater than or equal to 0).
    * @return the approximate quantiles at the given probabilities of each column
    */
  def rf_agg_approx_quantiles(
                               tile: Column,
                               probabilities: Seq[Double],
                               relativeError: Double = 0.00001): TypedColumn[Any, Seq[Double]] = {
    require(probabilities.nonEmpty, "at least one quantile probability is required")
    ApproxCellQuantilesAggregate(tile, probabilities, relativeError)
  }

  /** Compute the full column aggregate floating point statistics. */
  def rf_agg_stats(tile: Column): TypedColumn[Any, CellStatistics] = CellStatsAggregate(tile)

  /** Computes the column aggregate mean. */
  def rf_agg_mean(tile: Column): TypedColumn[Any, Double] = CellMeanAggregate(tile)

  /** Computes the number of non-NoData cells in a column. */
  def rf_agg_data_cells(tile: Column): TypedColumn[Any, Long] = CellCountAggregate.DataCells(tile)

  /** Computes the number of NoData cells in a column. */
  def rf_agg_no_data_cells(tile: Column): TypedColumn[Any, Long] = CellCountAggregate.NoDataCells(tile)

  /** Construct an overview raster of size `cols`x`rows` where data in `proj_raster` intersects the
    * `areaOfInterest` in web-mercator. Uses bi-linear sampling method. */
  def rf_agg_overview_raster(proj_raster: Column, cols: Int, rows: Int, areaOfInterest: Extent): TypedColumn[Any, Tile] =
    rf_agg_overview_raster(ExtractTile(proj_raster), GetExtent(proj_raster), GetCRS(proj_raster), cols, rows, areaOfInterest)

  /** Construct an overview raster of size `cols`x`rows` where data in `tile` intersects the `areaOfInterest` in web-mercator. Uses nearest bi-linear sampling method. */
  def rf_agg_overview_raster(tile: Column, tileExtent: Column, tileCRS: Column, cols: Int, rows: Int, areaOfInterest: Extent): TypedColumn[Any, Tile] =
    rf_agg_overview_raster(tile, tileExtent, tileCRS, cols, rows, areaOfInterest, ResampleMethod.DEFAULT)

  /** Construct an overview raster of size `cols`x`rows` where data in `tile` intersects the `areaOfInterest` in web-mercator.
    * Allows specification of one of these sampling methods:
    *   - geotrellis.raster.resample.NearestNeighbor
    *   - geotrellis.raster.resample.Bilinear
    *   - geotrellis.raster.resample.CubicConvolution
    *   - geotrellis.raster.resample.CubicSpline
    *   - geotrellis.raster.resample.Lanczos
    */
  def rf_agg_overview_raster(tile: Column, tileExtent: Column, tileCRS: Column, cols: Int, rows: Int, areaOfInterest: Extent, sampler: ResampleMethod): TypedColumn[Any, Tile] = {
    val params = ProjectedRasterDefinition(cols, rows, IntConstantNoDataCellType, WebMercator, areaOfInterest, sampler)
    TileRasterizerAggregate(params, tileCRS, tileExtent, tile)
  }

  import org.apache.spark.sql.functions._
  import org.locationtech.rasterframes.encoders.StandardEncoders.extentEncoder
  import org.locationtech.rasterframes.util.NamedColumn

  /** Compute the aggregate extent over a column. Assumes CRS homogeneity. */
  def rf_agg_extent(extent: Column): TypedColumn[Any, Extent] = {
    struct(
      min(extent.getField("xmin")) as "xmin",
      min(extent.getField("ymin")) as "ymin",
      max(extent.getField("xmax")) as "xmax",
      max(extent.getField("ymax")) as "ymax"
    ).as(s"rf_agg_extent(${extent.columnName})").as[Extent]
  }

  /** Compute the aggregate extent over a column after reprojecting from the rows source CRS into the given destination CRS . */
  def rf_agg_reprojected_extent(extent: Column, srcCRS: Column, destCRS: CRS): TypedColumn[Any, Extent] =
    rf_agg_extent(st_extent(st_reproject(st_geometry(extent), srcCRS, destCRS)))
    .as(s"rf_agg_reprojected_extent(${extent.columnName}, ${srcCRS.columnName}, $destCRS)")
    .as[Extent]
}
