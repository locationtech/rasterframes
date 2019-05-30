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

package org.locationtech.rasterframes
import geotrellis.proj4.CRS
import geotrellis.raster.mapalgebra.local.LocalTileBinaryOp
import geotrellis.raster.{CellType, Tile}
import geotrellis.vector.Extent
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.functions.{lit, udf}
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.jts.geom.Geometry
import org.locationtech.rasterframes.expressions.TileAssembler
import org.locationtech.rasterframes.expressions.accessors._
import org.locationtech.rasterframes.expressions.aggregates._
import org.locationtech.rasterframes.expressions.generators._
import org.locationtech.rasterframes.expressions.localops._
import org.locationtech.rasterframes.expressions.tilestats._
import org.locationtech.rasterframes.expressions.transformers._
import org.locationtech.rasterframes.model.TileDimensions
import org.locationtech.rasterframes.stats._
import org.locationtech.rasterframes.{functions => F}

/**
 * UDFs for working with Tiles in Spark DataFrames.
 *
 * @since 4/3/17
 */
trait RasterFunctions {
  import util._

  // format: off
  /** Create a row for each cell in Tile. */
  def rf_explode_tiles(cols: Column*): Column = rf_explode_tiles_sample(1.0, None, cols: _*)

  /** Create a row for each cell in Tile with random sampling and optional seed. */
  def rf_explode_tiles_sample(sampleFraction: Double, seed: Option[Long], cols: Column*): Column =
    ExplodeTiles(sampleFraction, seed, cols)

  /** Create a row for each cell in Tile with random sampling (no seed). */
  def rf_explode_tiles_sample(sampleFraction: Double, cols: Column*): Column =
    ExplodeTiles(sampleFraction, None, cols)

  /** Query the number of (cols, rows) in a Tile. */
  def rf_dimensions(col: Column): TypedColumn[Any, TileDimensions] = GetDimensions(col)

  /** Extracts the bounding box of a geometry as an Extent */
  def st_extent(col: Column): TypedColumn[Any, Extent] = GeometryToExtent(col)

  /** Extracts the bounding box from a RasterSource or ProjectedRasterTile */
  def rf_extent(col: Column): TypedColumn[Any, Extent] = GetExtent(col)

  /** Extracts the CRS from a RasterSource or ProjectedRasterTile */
  def rf_crs(col: Column): TypedColumn[Any, CRS] = GetCRS(col)

  /** Flattens Tile into a double array. */
  def rf_tile_to_array_double(col: Column): TypedColumn[Any, Array[Double]] =
    TileToArrayDouble(col)

  /** Flattens Tile into an integer array. */
  def rf_tile_to_array_int(col: Column): TypedColumn[Any, Array[Double]] =
    TileToArrayDouble(col)

  @Experimental
  /** Convert array in `arrayCol` into a Tile of dimensions `cols` and `rows`*/
  def rf_array_to_tile(arrayCol: Column, cols: Int, rows: Int) = withAlias("rf_array_to_tile", arrayCol)(
    udf[Tile, AnyRef](F.arrayToTile(cols, rows)).apply(arrayCol)
  )

  /** Create a Tile from a column of cell data with location indexes and preform cell conversion. */
  def rf_assemble_tile(columnIndex: Column, rowIndex: Column, cellData: Column, tileCols: Int, tileRows: Int, ct: CellType): TypedColumn[Any, Tile] =
    rf_convert_cell_type(TileAssembler(columnIndex, rowIndex, cellData, lit(tileCols), lit(tileRows)), ct).as(cellData.columnName).as[Tile](singlebandTileEncoder)

  /** Create a Tile from  a column of cell data with location indexes. */
  def rf_assemble_tile(columnIndex: Column, rowIndex: Column, cellData: Column, tileCols: Column, tileRows: Column): TypedColumn[Any, Tile] =
    TileAssembler(columnIndex, rowIndex, cellData, tileCols, tileRows)

  /** Extract the Tile's cell type */
  def rf_cell_type(col: Column): TypedColumn[Any, CellType] = GetCellType(col)

  /** Change the Tile's cell type */
  def rf_convert_cell_type(col: Column, cellType: CellType): TypedColumn[Any, Tile] =
    SetCellType(col, cellType)

  /** Change the Tile's cell type */
  def rf_convert_cell_type(col: Column, cellTypeName: String): TypedColumn[Any, Tile] =
    SetCellType(col, cellTypeName)

  /** Resample tile to different size based on scalar factor or tile whose dimension to match. Scalar less
    * than one will downsample tile; greater than one will upsample. Uses nearest-neighbor. */
  def rf_resample[T: Numeric](tileCol: Column, factorValue: T) = Resample(tileCol, factorValue)

  /** Resample tile to different size based on scalar factor or tile whose dimension to match. Scalar less
    * than one will downsample tile; greater than one will upsample. Uses nearest-neighbor. */
  def rf_resample(tileCol: Column, factorCol: Column) = Resample(tileCol, factorCol)

  /** Convert a bounding box structure to a Geometry type. Intented to support multiple schemas. */
  def st_geometry(extent: Column): TypedColumn[Any, Geometry] = ExtentToGeometry(extent)

  /** Extract the extent of a RasterSource or ProjectedRasterTile as a Geometry type. */
  def rf_geometry(raster: Column): TypedColumn[Any, Geometry] = GetGeometry(raster)

  /** Assign a `NoData` value to the Tiles. */
  def rf_with_no_data(col: Column, nodata: Double): TypedColumn[Any, Tile] = withTypedAlias("rf_with_no_data", col)(
    udf[Tile, Tile](F.withNoData(nodata)).apply(col)
  )

  /**  Compute the full column aggregate floating point histogram. */
  def rf_agg_approx_histogram(col: Column): TypedColumn[Any, CellHistogram] =
    HistogramAggregate(col)

  /** Compute the full column aggregate floating point statistics. */
  def rf_agg_stats(col: Column): TypedColumn[Any, CellStatistics] =
    CellStatsAggregate(col)

  /** Computes the column aggregate mean. */
  def rf_agg_mean(col: Column) = CellMeanAggregate(col)

  /** Computes the number of non-NoData cells in a column. */
  def rf_agg_data_cells(col: Column): TypedColumn[Any, Long] = CellCountAggregate.DataCells(col)

  /** Computes the number of NoData cells in a column. */
  def rf_agg_no_data_cells(col: Column): TypedColumn[Any, Long] = CellCountAggregate.NoDataCells(col)

  /** Compute the Tile-wise mean */
  def rf_tile_mean(col: Column): TypedColumn[Any, Double] =
    TileMean(col)

  /** Compute the Tile-wise sum */
  def rf_tile_sum(col: Column): TypedColumn[Any, Double] =
    Sum(col)

  /** Compute the minimum cell value in tile. */
  def rf_tile_min(col: Column): TypedColumn[Any, Double] =
    TileMin(col)

  /** Compute the maximum cell value in tile. */
  def rf_tile_max(col: Column): TypedColumn[Any, Double] =
    TileMax(col)

  /** Compute TileHistogram of Tile values. */
  def rf_tile_histogram(col: Column): TypedColumn[Any, CellHistogram] =
    TileHistogram(col)

  /** Compute statistics of Tile values. */
  def rf_tile_stats(col: Column): TypedColumn[Any, CellStatistics] =
    TileStats(col)

  /** Counts the number of non-NoData cells per Tile. */
  def rf_data_cells(tile: Column): TypedColumn[Any, Long] =
    DataCells(tile)

  /** Counts the number of NoData cells per Tile. */
  def rf_no_data_cells(tile: Column): TypedColumn[Any, Long] =
    NoDataCells(tile)

  /** Returns true if all cells in the tile are NoData.*/
  def rf_is_no_data_tile(tile: Column): TypedColumn[Any, Boolean] =
    IsNoDataTile(tile)

  /** Returns true if any cells in the tile are true (non-zero and not NoData). */
  def rf_exists(tile: Column): TypedColumn[Any, Boolean] = Exists(tile)

  /** Returns true if all cells in the tile are true (non-zero and not NoData). */
  def rf_for_all(tile: Column): TypedColumn[Any, Boolean] = ForAll(tile)

  /** Compute cell-local aggregate descriptive statistics for a column of Tiles. */
  def rf_agg_local_stats(col: Column) =
    LocalStatsAggregate(col)

  /** Compute the cell-wise/local max operation between Tiles in a column. */
  def rf_agg_local_max(col: Column): TypedColumn[Any, Tile] = LocalTileOpAggregate.LocalMaxUDAF(col)

  /** Compute the cellwise/local min operation between Tiles in a column. */
  def rf_agg_local_min(col: Column): TypedColumn[Any, Tile] = LocalTileOpAggregate.LocalMinUDAF(col)

  /** Compute the cellwise/local mean operation between Tiles in a column. */
  def rf_agg_local_mean(col: Column): TypedColumn[Any, Tile] = LocalMeanAggregate(col)

  /** Compute the cellwise/local count of non-NoData cells for all Tiles in a column. */
  def rf_agg_local_data_cells(col: Column): TypedColumn[Any, Tile] = LocalCountAggregate.LocalDataCellsUDAF(col)

  /** Compute the cellwise/local count of NoData cells for all Tiles in a column. */
  def rf_agg_local_no_data_cells(col: Column): TypedColumn[Any, Tile] = LocalCountAggregate.LocalNoDataCellsUDAF(col)

  /** Cellwise addition between two Tiles or Tile and scalar column. */
  def rf_local_add(left: Column, right: Column): TypedColumn[Any, Tile] = Add(left, right)

  /** Cellwise addition of a scalar value to a tile. */
  def rf_local_add[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = Add(tileCol, value)

  /** Cellwise subtraction between two Tiles. */
  def rf_local_subtract(left: Column, right: Column): TypedColumn[Any, Tile] = Subtract(left, right)

  /** Cellwise subtraction of a scalar value from a tile. */
  def rf_local_subtract[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile]  = Subtract(tileCol, value)

  /** Cellwise multiplication between two Tiles. */
  def rf_local_multiply(left: Column, right: Column): TypedColumn[Any, Tile] = Multiply(left, right)

  /** Cellwise multiplication of a tile by a scalar value. */
  def rf_local_multiply[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = Multiply(tileCol, value)

  /** Cellwise division between two Tiles. */
  def rf_local_divide(left: Column, right: Column): TypedColumn[Any, Tile] = Divide(left, right)

  /** Cellwise division of a tile by a scalar value. */
  def rf_local_divide[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = Divide(tileCol, value)

  /** Perform an arbitrary GeoTrellis `LocalTileBinaryOp` between two Tile columns. */
  def rf_local_algebra(op: LocalTileBinaryOp, left: Column, right: Column): TypedColumn[Any, Tile] =
    withTypedAlias(opName(op), left, right)(udf[Tile, Tile, Tile](op.apply).apply(left, right))

  /** Compute the normalized difference of two tile columns */
  def rf_normalized_difference(left: Column, right: Column) =
    NormalizedDifference(left, right)

  /** Constructor for tile column with a single cell value. */
  def rf_make_constant_tile(value: Number, cols: Int, rows: Int, cellType: CellType): TypedColumn[Any, Tile] =
    rf_make_constant_tile(value, cols, rows, cellType.name)

  /** Constructor for tile column with a single cell value. */
  def rf_make_constant_tile(value: Number, cols: Int, rows: Int, cellTypeName: String): TypedColumn[Any, Tile] = {
    import org.apache.spark.sql.rf.TileUDT.tileSerializer
    val constTile = encoders.serialized_literal(F.makeConstantTile(value, cols, rows, cellTypeName))
    withTypedAlias(s"rf_make_constant_tile($value, $cols, $rows, $cellTypeName)")(constTile)
  }

  /** Create a column constant tiles of zero */
  def rf_make_zeros_tile(cols: Int, rows: Int, cellType: CellType): TypedColumn[Any, Tile] =
    rf_make_zeros_tile(cols, rows, cellType.name)

  /** Create a column constant tiles of zero */
  def rf_make_zeros_tile(cols: Int, rows: Int, cellTypeName: String): TypedColumn[Any, Tile] = {
    import org.apache.spark.sql.rf.TileUDT.tileSerializer
    val constTile = encoders.serialized_literal(F.tileZeros(cols, rows, cellTypeName))
    withTypedAlias(s"rf_make_zeros_tile($cols, $rows, $cellTypeName)")(constTile)
  }

  /** Creates a column of tiles containing all ones */
  def rf_make_ones_tile(cols: Int, rows: Int, cellType: CellType): TypedColumn[Any, Tile] =
    rf_make_ones_tile(cols, rows, cellType.name)

  /** Creates a column of tiles containing all ones */
  def rf_make_ones_tile(cols: Int, rows: Int, cellTypeName: String): TypedColumn[Any, Tile] = {
    import org.apache.spark.sql.rf.TileUDT.tileSerializer
    val constTile = encoders.serialized_literal(F.tileOnes(cols, rows, cellTypeName))
    withTypedAlias(s"rf_make_ones_tile($cols, $rows, $cellTypeName)")(constTile)
  }

  /** Where the rf_mask tile contains NODATA, replace values in the source tile with NODATA */
  def rf_mask(sourceTile: Column, maskTile: Column): TypedColumn[Any, Tile] =
    Mask.MaskByDefined(sourceTile, maskTile)

  /** Where the `maskTile` equals `maskValue`, replace values in the source tile with `NoData` */
  def rf_mask_by_value(sourceTile: Column, maskTile: Column, maskValue: Column): TypedColumn[Any, Tile] =
    Mask.MaskByValue(sourceTile, maskTile, maskValue)

  /** Where the `maskTile` does **not** contain `NoData`, replace values in the source tile with `NoData` */
  def rf_inverse_mask(sourceTile: Column, maskTile: Column): TypedColumn[Any, Tile] =
    Mask.InverseMaskByDefined(sourceTile, maskTile)

  /** Where the `maskTile` does **not** equal `maskValue`, replace values in the source tile with `NoData` */
  def rf_inverse_mask_by_value(sourceTile: Column, maskTile: Column, maskValue: Column): TypedColumn[Any, Tile] =
    Mask.InverseMaskByValue(sourceTile, maskTile, maskValue)

  /** Create a tile where cells in the grid defined by cols, rows, and bounds are filled with the given value. */
  def rf_rasterize(geometry: Column, bounds: Column, value: Column, cols: Int, rows: Int): TypedColumn[Any, Tile] =
    withTypedAlias("rf_rasterize", geometry)(
      udf(F.rasterize(_: Geometry, _: Geometry, _: Int, cols, rows)).apply(geometry, bounds, value)
    )

  /** Reproject a column of geometry from one CRS to another.
    * @param sourceGeom Geometry column to reproject
    * @param srcCRS Native CRS of `sourceGeom` as a literal
    * @param dstCRSCol Destination CRS as a column
    */
  def st_reproject(sourceGeom: Column, srcCRS: CRS, dstCRSCol: Column): TypedColumn[Any, Geometry] =
    ReprojectGeometry(sourceGeom, srcCRS, dstCRSCol)

  /** Reproject a column of geometry from one CRS to another.
    * @param sourceGeom Geometry column to reproject
    * @param srcCRSCol Native CRS of `sourceGeom` as a column
    * @param dstCRS Destination CRS as a literal
    */
    def st_reproject(sourceGeom: Column, srcCRSCol: Column, dstCRS: CRS): TypedColumn[Any, Geometry] =
    ReprojectGeometry(sourceGeom, srcCRSCol, dstCRS)

  /** Reproject a column of geometry from one CRS to another.
    * @param sourceGeom Geometry column to reproject
    * @param srcCRS Native CRS of `sourceGeom` as a literal
    * @param dstCRS Destination CRS as a literal
    */
  def st_reproject(sourceGeom: Column, srcCRS: CRS, dstCRS: CRS): TypedColumn[Any, Geometry] =
    ReprojectGeometry(sourceGeom, srcCRS, dstCRS)

  /** Reproject a column of geometry from one CRS to another.
    * @param sourceGeom Geometry column to reproject
    * @param srcCRSCol Native CRS of `sourceGeom` as a column
    * @param dstCRSCol Destination CRS as a column
    */
  def st_reproject(sourceGeom: Column, srcCRSCol: Column, dstCRSCol: Column): TypedColumn[Any, Geometry] =
    ReprojectGeometry(sourceGeom, srcCRSCol, dstCRSCol)

  /** Render Tile as ASCII string, for debugging purposes. */
  def rf_render_ascii(col: Column): TypedColumn[Any, String] =
    DebugRender.RenderAscii(col)

  /** Render Tile cell values as numeric values, for debugging purposes. */
  def rf_render_matrix(col: Column): TypedColumn[Any, String] =
    DebugRender.RenderMatrix(col)

  /** Cellwise less than value comparison between two tiles. */
  def rf_local_less(left: Column, right: Column): TypedColumn[Any, Tile] =
    Less(left, right)

  /** Cellwise less than value comparison between a tile and a scalar. */
  def rf_local_less[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] =
    Less(tileCol, value)

  /** Cellwise less than or equal to value comparison between a tile and a scalar. */
  def rf_local_less_equal(left: Column, right: Column): TypedColumn[Any, Tile]  =
    LessEqual(left, right)

  /** Cellwise less than or equal to value comparison between a tile and a scalar. */
  def rf_local_less_equal[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] =
    LessEqual(tileCol, value)

  /** Cellwise greater than value comparison between two tiles. */
  def rf_local_greater(left: Column, right: Column): TypedColumn[Any, Tile] =
    Greater(left, right)

  /** Cellwise greater than value comparison between a tile and a scalar. */
  def rf_local_greater[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] =
    Greater(tileCol, value)

  /** Cellwise greater than or equal to value comparison between two tiles. */
  def rf_local_greater_equal(left: Column, right: Column): TypedColumn[Any, Tile]  =
    GreaterEqual(left, right)

  /** Cellwise greater than or equal to value comparison between a tile and a scalar. */
  def rf_local_greater_equal[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] =
    GreaterEqual(tileCol, value)

  /** Cellwise equal to value comparison between two tiles. */
  def rf_local_equal(left: Column, right: Column): TypedColumn[Any, Tile]  =
    Equal(left, right)

  /** Cellwise equal to value comparison between a tile and a scalar. */
  def rf_local_equal[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] =
    Equal(tileCol, value)

  /** Cellwise inequality comparison between two tiles. */
  def rf_local_unequal(left: Column, right: Column): TypedColumn[Any, Tile]  =
    Unequal(left, right)

  /** Cellwise inequality comparison between a tile and a scalar. */
  def rf_local_unequal[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] =
    Unequal(tileCol, value)

  /** Round cell values to nearest integer without chaning cell type. */
  def rf_round(tileCol: Column): TypedColumn[Any, Tile] =
    Round(tileCol)

  /** Compute the absolute value of each cell. */
  def rf_abs(tileCol: Column): TypedColumn[Any, Tile] =
    Abs(tileCol)

  /** Take natural logarithm of cell values. */
  def rf_log(tileCol: Column): TypedColumn[Any, Tile] =
    Log(tileCol)

  /** Take base 10 logarithm of cell values. */
  def rf_log10(tileCol: Column): TypedColumn[Any, Tile] =
    Log10(tileCol)

  /** Take base 2 logarithm of cell values. */
  def rf_log2(tileCol: Column): TypedColumn[Any, Tile] =
    Log2(tileCol)

  /** Natural logarithm of one plus cell values. */
  def rf_log1p(tileCol: Column): TypedColumn[Any, Tile] =
      Log1p(tileCol)

  /** Exponential of cell values */
  def rf_exp(tileCol: Column): TypedColumn[Any, Tile] =
    Exp(tileCol)

  /** Ten to the power of cell values */
  def rf_exp10(tileCol: Column): TypedColumn[Any, Tile] =
    Exp10(tileCol)

  /** Two to the power of cell values */
  def rf_exp2(tileCol: Column): TypedColumn[Any, Tile] =
    Exp2(tileCol)

  /** Exponential of cell values, less one*/
  def rf_expm1(tileCol: Column): TypedColumn[Any, Tile] =
    ExpM1(tileCol)

  def rf_identity(tileCol: Column): TypedColumn[Any, Tile] =
    Identity(tileCol)

}
