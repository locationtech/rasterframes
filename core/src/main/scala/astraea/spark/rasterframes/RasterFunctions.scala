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

package astraea.spark.rasterframes

import astraea.spark.rasterframes.expressions.TileAssembler
import astraea.spark.rasterframes.expressions.accessors._
import astraea.spark.rasterframes.expressions.aggstats._
import astraea.spark.rasterframes.expressions.generators._
import astraea.spark.rasterframes.expressions.localops._
import astraea.spark.rasterframes.expressions.tilestats._
import astraea.spark.rasterframes.expressions.transformers._
import astraea.spark.rasterframes.stats.{CellHistogram, CellStatistics}
import astraea.spark.rasterframes.{functions => F}
import geotrellis.proj4.CRS
import geotrellis.raster.mapalgebra.local.LocalTileBinaryOp
import geotrellis.raster.{CellType, Tile}
import geotrellis.vector.Extent
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.locationtech.jts.geom.{Envelope, Geometry}

/**
 * UDFs for working with Tiles in Spark DataFrames.
 *
 * @since 4/3/17
 */
trait RasterFunctions {
  import util._

  // format: off
  /** Create a row for each cell in Tile. */
  def explode_tiles(cols: Column*): Column = explode_tiles_sample(1.0, None, cols: _*)

  /** Create a row for each cell in Tile with random sampling and optional seed. */
  def explode_tiles_sample(sampleFraction: Double, seed: Option[Long], cols: Column*): Column =
    ExplodeTiles(sampleFraction, seed, cols)

  /** Create a row for each cell in Tile with random sampling (no seed). */
  def explode_tiles_sample(sampleFraction: Double, cols: Column*): Column =
    ExplodeTiles(sampleFraction, None, cols)

  /** Query the number of (cols, rows) in a Tile. */
  def tile_dimensions(col: Column): Column = GetDimensions(col)

  /** Extracts the bounding box of a geometry as a JTS envelope. */
  @deprecated("Replace usages of this with `st_extent`", "11/4/2018")
  def envelope(col: Column): TypedColumn[Any, Envelope] = GetEnvelope(col)

  /** Extracts the bounding box of a geometry as an Extent */
  def st_extent(col: Column): TypedColumn[Any, Extent] = GeometryToExtent(col)

  /** Flattens Tile into a double array. */
  def tile_to_array_double(col: Column): TypedColumn[Any, Array[Double]] =
    TileToArrayDouble(col)

  /** Flattens Tile into an integer array. */
  def tile_to_array_int(col: Column): TypedColumn[Any, Array[Double]] =
    TileToArrayDouble(col)

  @Experimental
  /** Convert array in `arrayCol` into a Tile of dimensions `cols` and `rows`*/
  def array_to_tile(arrayCol: Column, cols: Int, rows: Int) = withAlias("array_to_tile", arrayCol)(
    udf[Tile, AnyRef](F.arrayToTile(cols, rows)).apply(arrayCol)
  )

  /** Create a Tile from a column of cell data with location indexes and preform cell conversion. */
  def assemble_tile(columnIndex: Column, rowIndex: Column, cellData: Column, tileCols: Int, tileRows: Int, ct: CellType): TypedColumn[Any, Tile] =
    convert_cell_type(TileAssembler(columnIndex, rowIndex, cellData, lit(tileCols), lit(tileRows)), ct).as(cellData.columnName).as[Tile](singlebandTileEncoder)

  /** Create a Tile from  a column of cell data with location indexes. */
  def assemble_tile(columnIndex: Column, rowIndex: Column, cellData: Column, tileCols: Column, tileRows: Column): TypedColumn[Any, Tile] =
    TileAssembler(columnIndex, rowIndex, cellData, tileCols, tileRows)

  /** Extract the Tile's cell type */
  def cell_type(col: Column): TypedColumn[Any, CellType] = GetCellType(col)

  /** Change the Tile's cell type */
  def convert_cell_type(col: Column, cellType: CellType): TypedColumn[Any, Tile] =
    SetCellType(col, cellType)

  /** Change the Tile's cell type */
  def convert_cell_type(col: Column, cellTypeName: String): TypedColumn[Any, Tile] =
    SetCellType(col, cellTypeName)

  /** Convert a bounding box structure to a Geometry type. Intented to support multiple schemas. */
  def bounds_geometry(bounds: Column): TypedColumn[Any, Geometry] = BoundsToGeometry(bounds)

  /** Assign a `NoData` value to the Tiles. */
  def with_no_data(col: Column, nodata: Double): TypedColumn[Any, Tile] = withAlias("with_no_data", col)(
    udf[Tile, Tile](F.withNoData(nodata)).apply(col)
  ).as[Tile]

  /**  Compute the full column aggregate floating point histogram. */
  def agg_approx_histogram(col: Column): TypedColumn[Any, CellHistogram] =
    HistogramAggregate(col)

  /** Compute the full column aggregate floating point statistics. */
  def agg_stats(col: Column): TypedColumn[Any, CellStatistics] =
    CellStatsAggregate(col)

  /** Computes the column aggregate mean. */
  def agg_mean(col: Column) = CellMeanAggregate(col)

  /** Computes the number of non-NoData cells in a column. */
  def agg_data_cells(col: Column): TypedColumn[Any, Long] = CellCountAggregate.DataCells(col)

  /** Computes the number of NoData cells in a column. */
  def agg_no_data_cells(col: Column): TypedColumn[Any, Long] = CellCountAggregate.NoDataCells(col)

  /** Compute the Tile-wise mean */
  def tile_mean(col: Column): TypedColumn[Any, Double] =
    TileMean(col)

  /** Compute the Tile-wise sum */
  def tile_sum(col: Column): TypedColumn[Any, Double] =
    Sum(col)

  /** Compute the minimum cell value in tile. */
  def tile_min(col: Column): TypedColumn[Any, Double] =
    TileMin(col)

  /** Compute the maximum cell value in tile. */
  def tile_max(col: Column): TypedColumn[Any, Double] =
    TileMax(col)

  /** Compute TileHistogram of Tile values. */
  def tile_histogram(col: Column): TypedColumn[Any, CellHistogram] =
    TileHistogram(col)

  /** Compute statistics of Tile values. */
  def tile_stats(col: Column): TypedColumn[Any, CellStatistics] =
    TileStats(col)

  /** Counts the number of non-NoData cells per Tile. */
  def data_cells(tile: Column): TypedColumn[Any, Long] =
    DataCells(tile)

  /** Counts the number of NoData cells per Tile. */
  def no_data_cells(tile: Column): TypedColumn[Any, Long] =
    NoDataCells(tile)

  def is_no_data_tile(tile: Column): TypedColumn[Any, Boolean] =
    IsNoDataTile(tile)

  /** Compute cell-local aggregate descriptive statistics for a column of Tiles. */
  def agg_local_stats(col: Column) =
    LocalStatsAggregate(col)

  /** Compute the cell-wise/local max operation between Tiles in a column. */
  def agg_local_max(col: Column): TypedColumn[Any, Tile] = LocalTileOpAggregate.LocalMaxUDAF(col)

  /** Compute the cellwise/local min operation between Tiles in a column. */
  def agg_local_min(col: Column): TypedColumn[Any, Tile] = LocalTileOpAggregate.LocalMinUDAF(col)

  /** Compute the cellwise/local mean operation between Tiles in a column. */
  def agg_local_mean(col: Column): TypedColumn[Any, Tile] = LocalMeanAggregate(col)

  /** Compute the cellwise/local count of non-NoData cells for all Tiles in a column. */
  def agg_local_data_cells(col: Column): TypedColumn[Any, Tile] = LocalCountAggregate.LocalDataCellsUDAF(col)

  /** Compute the cellwise/local count of NoData cells for all Tiles in a column. */
  def agg_local_no_data_cells(col: Column): TypedColumn[Any, Tile] = LocalCountAggregate.LocalNoDataCellsUDAF(col)

  /** Cellwise addition between two Tiles or Tile and scalar column. */
  def local_add(left: Column, right: Column): TypedColumn[Any, Tile] = Add(left, right)

  /** Cellwise addition of a scalar value to a tile. */
  def local_add[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = Add(tileCol, value)

  /** Cellwise subtraction between two Tiles. */
  def local_subtract(left: Column, right: Column): TypedColumn[Any, Tile] = Subtract(left, right)

  /** Cellwise subtraction of a scalar value from a tile. */
  def local_subtract[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile]  = Subtract(tileCol, value)

  /** Cellwise multiplication between two Tiles. */
  def local_multiply(left: Column, right: Column): TypedColumn[Any, Tile] = Multiply(left, right)

  /** Cellwise multiplication of a tile by a scalar value. */
  def local_multiply[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = Multiply(tileCol, value)

  /** Cellwise division between two Tiles. */
  def local_divide(left: Column, right: Column): TypedColumn[Any, Tile] = Divide(left, right)

  /** Cellwise division of a tile by a scalar value. */
  def local_divide[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = Divide(tileCol, value)

  /** Perform an arbitrary GeoTrellis `LocalTileBinaryOp` between two Tile columns. */
  def local_algebra(op: LocalTileBinaryOp, left: Column, right: Column):
  TypedColumn[Any, Tile] =
    withAlias(opName(op), left, right)(
      udf[Tile, Tile, Tile](op.apply).apply(left, right)
    ).as[Tile]

  /** Compute the normalized difference of two tile columns */
  def normalized_difference(left: Column, right: Column) =
    NormalizedDifference(left, right)

  /** Constructor for constant tile column */
  def make_constant_tile(value: Number, cols: Int, rows: Int, cellType: String): TypedColumn[Any, Tile] =
    udf(() => F.makeConstantTile(value, cols, rows, cellType)).apply().as(s"constant_$cellType").as[Tile]

  /** Alias for column of constant tiles of zero */
  def tile_zeros(cols: Int, rows: Int, cellType: String = "float64"): TypedColumn[Any, Tile] =
    udf(() => F.tileZeros(cols, rows, cellType)).apply().as(s"zeros_$cellType").as[Tile]

  /** Alias for column of constant tiles of one */
  def tile_ones(cols: Int, rows: Int, cellType: String = "float64"): TypedColumn[Any, Tile] =
    udf(() => F.tileOnes(cols, rows, cellType)).apply().as(s"ones_$cellType").as[Tile]

  /** Where the mask tile contains NODATA, replace values in the source tile with NODATA */
  def mask(sourceTile: Column, maskTile: Column): TypedColumn[Any, Tile] =
    Mask.MaskByDefined(sourceTile, maskTile)

  /** Where the mask tile equals the mask value, replace values in the source tile with NODATA */
  def mask_by_value(sourceTile: Column, maskTile: Column, maskValue: Column): TypedColumn[Any, Tile] =
    Mask.MaskByValue(sourceTile, maskTile, maskValue)

  /** Where the mask tile DOES NOT contain NODATA, replace values in the source tile with NODATA */
  def inverse_mask(sourceTile: Column, maskTile: Column): TypedColumn[Any, Tile] =
    Mask.InverseMaskByDefined(sourceTile, maskTile)

  /** Create a tile where cells in the grid defined by cols, rows, and bounds are filled with the given value. */
  def rasterize(geometry: Column, bounds: Column, value: Column, cols: Int, rows: Int): TypedColumn[Any, Tile] =
    withAlias("rasterize", geometry)(
      udf(F.rasterize(_: Geometry, _: Geometry, _: Int, cols, rows)).apply(geometry, bounds, value)
    ).as[Tile]

  /** Reproject a column of geometry from one CRS to another. */
  def reproject_geometry(sourceGeom: Column, srcCRS: CRS, dstCRSCol: Column): TypedColumn[Any, Geometry] =
    ReprojectGeometry(sourceGeom, srcCRS, dstCRSCol)

  /** Reproject a column of geometry from one CRS to another. */
  def reproject_geometry(sourceGeom: Column, srcCRSCol: Column, dstCRS: CRS): TypedColumn[Any, Geometry] =
    ReprojectGeometry(sourceGeom, srcCRSCol, dstCRS)

  /** Reproject a column of geometry from one CRS to another. */
  def reproject_geometry(sourceGeom: Column, srcCRS: CRS, dstCRS: CRS): TypedColumn[Any, Geometry] =
    ReprojectGeometry(sourceGeom, srcCRS, dstCRS)

  /** Render Tile as ASCII string, for debugging purposes. */
  def render_ascii(col: Column): TypedColumn[Any, String] =
    DebugRender.RenderAscii(col)

  /** Render Tile cell values as numeric values, for debugging purposes. */
  def render_matrix(col: Column): TypedColumn[Any, String] =
    DebugRender.RenderMatrix(col)

  /** Cellwise less than value comparison between two tiles. */
  def local_less(left: Column, right: Column): TypedColumn[Any, Tile] =
    Less(left, right)

  /** Cellwise less than value comparison between a tile and a scalar. */
  def local_less[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] =
    Less(tileCol, value)

  /** Cellwise less than or equal to value comparison between a tile and a scalar. */
  def local_less_equal(left: Column, right: Column): TypedColumn[Any, Tile]  =
    LessEqual(left, right)

  /** Cellwise less than or equal to value comparison between a tile and a scalar. */
  def local_less_equal[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] =
    LessEqual(tileCol, value)

  /** Cellwise greater than value comparison between two tiles. */
  def local_greater(left: Column, right: Column): TypedColumn[Any, Tile] =
    Greater(left, right)

  /** Cellwise greater than value comparison between a tile and a scalar. */
  def local_greater[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] =
    Greater(tileCol, value)

  /** Cellwise greater than or equal to value comparison between two tiles. */
  def local_greater_equal(left: Column, right: Column): TypedColumn[Any, Tile]  =
    GreaterEqual(left, right)

  /** Cellwise greater than or equal to value comparison between a tile and a scalar. */
  def local_greater_equal[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] =
    GreaterEqual(tileCol, value)

  /** Cellwise equal to value comparison between two tiles. */
  def local_equal(left: Column, right: Column): TypedColumn[Any, Tile]  =
    Equal(left, right)

  /** Cellwise equal to value comparison between a tile and a scalar. */
  def local_equal[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] =
    Equal(tileCol, value)

  /** Cellwise inequality comparison between two tiles. */
  def local_unequal(left: Column, right: Column): TypedColumn[Any, Tile]  =
    Unequal(left, right)

  /** Cellwise inequality comparison between a tile and a scalar. */
  def local_unequal[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] =
    Unequal(tileCol, value)

  /** Round cell values to nearest integer without chaning cell type. */
  def round(tileCol: Column): TypedColumn[Any, Tile] =
    Round(tileCol)

  /** Take natural logarithm of cell values. */
  def log(tileCol: Column): TypedColumn[Any, Tile] =
    Log(tileCol)

  /** Take base 10 logarithm of cell values. */
  def log10(tileCol: Column): TypedColumn[Any, Tile] =
    Log10(tileCol)

  /** Take base 2 logarithm of cell values. */
  def log2(tileCol: Column): TypedColumn[Any, Tile] =
    Log2(tileCol)

  /** Natural logarithm of one plus cell values. */
  def log1p(tileCol: Column): TypedColumn[Any, Tile] =
      Log1p(tileCol)

  /** Exponential of cell values */
  def exp(tileCol: Column): TypedColumn[Any, Tile] =
    Exp(tileCol)

  /** Ten to the power of cell values */
  def exp10(tileCol: Column): TypedColumn[Any, Tile] =
    Exp10(tileCol)

  /** Two to the power of cell values */
  def exp2(tileCol: Column): TypedColumn[Any, Tile] =
    Exp2(tileCol)

  /** Exponential of cell values, less one*/
  def expm1(tileCol: Column): TypedColumn[Any, Tile] =
    ExpM1(tileCol)

  /** Resample tile using nearest-neighbor */
  def resample[T: Numeric](tileCol: Column, value: T) = Resample(tileCol, value)

  /** Resample tile using nearest-neighbor */
  def resample(tileCol: Column, column2: Column) = Resample(tileCol, column2)

}
