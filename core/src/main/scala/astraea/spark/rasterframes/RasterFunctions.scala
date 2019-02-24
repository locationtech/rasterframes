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

import astraea.spark.rasterframes.encoders.SparkDefaultEncoders
import astraea.spark.rasterframes.expressions.mapalgebra._
import astraea.spark.rasterframes.expressions.generators._
import astraea.spark.rasterframes.expressions.accessors._
import astraea.spark.rasterframes.expressions.transformers._
import astraea.spark.rasterframes.functions.{CellCountAggregate, CellMeanAggregate}
import astraea.spark.rasterframes.stats.{CellHistogram, CellStatistics}
import astraea.spark.rasterframes.{functions => F}
import com.vividsolutions.jts.geom.{Envelope, Geometry}
import geotrellis.proj4.CRS
import geotrellis.raster.mapalgebra.local.LocalTileBinaryOp
import geotrellis.raster.{CellType, Tile}
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.reflect.runtime.universe._

/**
 * UDFs for working with Tiles in Spark DataFrames.
 *
 * @since 4/3/17
 */
trait RasterFunctions {
  import SparkDefaultEncoders._
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
  def envelope(col: Column): TypedColumn[Any, Envelope] = GetEnvelope(col)

  /** Flattens Tile into an array. A numeric type parameter is required. */
  @Experimental
  def tile_to_array[T: HasCellType: TypeTag](col: Column): TypedColumn[Any, Array[T]] = withAlias("tile_to_array", col)(
    udf[Array[T], Tile](F.tileToArray).apply(col)
  ).as[Array[T]]

  @Experimental
  /** Convert array in `arrayCol` into a Tile of dimensions `cols` and `rows`*/
  def array_to_tile(arrayCol: Column, cols: Int, rows: Int) = withAlias("array_to_tile", arrayCol)(
    udf[Tile, AnyRef](F.arrayToTile(cols, rows)).apply(arrayCol)
  )

  /** Create a Tile from a column of cell data with location indexes and preform cell conversion. */
  def assemble_tile(columnIndex: Column, rowIndex: Column, cellData: Column, tileCols: Int, tileRows: Int, ct: CellType): TypedColumn[Any, Tile] =
    convert_cell_type(F.TileAssembler(columnIndex, rowIndex, cellData, lit(tileCols), lit(tileRows)), ct).as(cellData.columnName).as[Tile]

  /** Create a Tile from  a column of cell data with location indexes. */
  def assemble_tile(columnIndex: Column, rowIndex: Column, cellData: Column, tileCols: Column, tileRows: Column): TypedColumn[Any, Tile] =
    F.TileAssembler(columnIndex, rowIndex, cellData, tileCols, tileRows)

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
  def with_no_data(col: Column, nodata: Double) = withAlias("with_no_data", col)(
    udf[Tile, Tile](F.withNoData(nodata)).apply(col)
  ).as[Tile]

  /**  Compute the full column aggregate floating point histogram. */
  def agg_histogram(col: Column): TypedColumn[Any, CellHistogram] =
  withAlias("histogram", col)(
    F.aggHistogram(col)
  ).as[CellHistogram]

  /** Compute the full column aggregate floating point statistics. */
  def agg_stats(col: Column): TypedColumn[Any, CellStatistics] = withAlias("agg_stats", col)(
    F.aggStats(col)
  ).as[CellStatistics]

  /** Computes the column aggregate mean. */
  def agg_mean(col: Column) = CellMeanAggregate(col)

  /** Computes the number of non-NoData cells in a column. */
  def agg_data_cells(col: Column) = CellCountAggregate(true, col)

  /** Computes the number of NoData cells in a column. */
  def agg_no_data_cells(col: Column) = CellCountAggregate(false, col)

  /** Compute the Tile-wise mean */
  def tile_mean(col: Column): TypedColumn[Any, Double] =
  withAlias("tile_mean", col)(
    udf[Double, Tile](F.tileMean).apply(col)
  ).as[Double]

  /** Compute the Tile-wise sum */
  def tile_sum(col: Column): TypedColumn[Any, Double] =
  withAlias("tile_sum", col)(
    udf[Double, Tile](F.tileSum).apply(col)
  ).as[Double]

  /** Compute the minimum cell value in tile. */
  def tile_min(col: Column): TypedColumn[Any, Double] =
  withAlias("tile_min", col)(
    udf[Double, Tile](F.tileMin).apply(col)
  ).as[Double]

  /** Compute the maximum cell value in tile. */
  def tile_max(col: Column): TypedColumn[Any, Double] =
  withAlias("tile_max", col)(
    udf[Double, Tile](F.tileMax).apply(col)
  ).as[Double]

  /** Compute TileHistogram of Tile values. */
  def tile_histogram(col: Column): TypedColumn[Any, CellHistogram] =
  withAlias("tile_histogram", col)(
    udf[CellHistogram, Tile](F.tileHistogram).apply(col)
  ).as[CellHistogram]

  /** Compute statistics of Tile values. */
  def tile_stats(col: Column): TypedColumn[Any, CellStatistics] =
  withAlias("tile_stats", col)(
    udf[CellStatistics, Tile](F.tileStats).apply(col)
  ).as[CellStatistics]

  /** Counts the number of non-NoData cells per Tile. */
  def data_cells(tile: Column): TypedColumn[Any, Long] =
    withAlias("data_cells", tile)(
      udf(F.dataCells).apply(tile)
    ).as[Long]

  /** Counts the number of NoData cells per Tile. */
  def no_data_cells(tile: Column): TypedColumn[Any, Long] =
    withAlias("no_data_cells", tile)(
      udf(F.noDataCells).apply(tile)
    ).as[Long]


  def is_no_data_tile(tile: Column): TypedColumn[Any, Boolean] =
    withAlias("is_no_data_tile", tile)(
      udf(F.isNoDataTile).apply(tile)
    ).as[Boolean]

  /** Compute cell-local aggregate descriptive statistics for a column of Tiles. */
  def local_agg_stats(col: Column): Column =
  withAlias("local_agg_stats", col)(
    F.localAggStats(col)
  )

  /** Compute the cell-wise/local max operation between Tiles in a column. */
  def local_agg_max(col: Column): TypedColumn[Any, Tile] =
  withAlias("local_agg_max", col)(
    F.localAggMax(col)
  ).as[Tile]

  /** Compute the cellwise/local min operation between Tiles in a column. */
  def local_agg_min(col: Column): TypedColumn[Any, Tile] =
  withAlias("local_agg_min", col)(
    F.localAggMin(col)
  ).as[Tile]

  /** Compute the cellwise/local mean operation between Tiles in a column. */
  def local_agg_mean(col: Column): TypedColumn[Any, Tile] =
  withAlias("local_agg_mean", col)(
    F.localAggMean(col)
  ).as[Tile]

  /** Compute the cellwise/local count of non-NoData cells for all Tiles in a column. */
  def local_agg_data_cells(col: Column): TypedColumn[Any, Tile] =
  withAlias("local_agg_data_cells", col)(
    F.localAggCount(col)
  ).as[Tile]

  /** Compute the cellwise/local count of NoData cells for all Tiles in a column. */
  def local_agg_no_data_cells(col: Column): TypedColumn[Any, Tile] =
  withAlias("local_agg_no_data_cells", col)(
    F.localAggNodataCount(col)
  ).as[Tile]

  /** Cellwise addition between two Tiles or Tile and scalar column. */
  def local_add(left: Column, right: Column): Column =
    Add(left, right)

  /** Cellwise addition of a scalar value to a tile. */
  def local_add[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = Add(tileCol, value)

  /** Cellwise subtraction between two Tiles. */
  def local_subtract(left: Column, right: Column): TypedColumn[Any, Tile] =
    Subtract(left, right)

  /** Cellwise subtraction of a scalar value from a tile. */
  def local_subtract[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile]  =
    Subtract(tileCol, value)

  /** Cellwise multiplication between two Tiles. */
  def local_multiply(left: Column, right: Column): TypedColumn[Any, Tile] =
    Multiply(left, right)

  /** Cellwise multiplication of a tile by a scalar value. */
  def local_multiply[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] =
    Multiply(tileCol, value)

  /** Cellwise division between two Tiles. */
  def local_divide(left: Column, right: Column): TypedColumn[Any, Tile] =
    Divide(left, right)

  /** Cellwise division of a tile by a scalar value. */
  def local_divide[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] =
    Divide(tileCol, value)

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
    withAlias("mask", sourceTile, maskTile)(
      udf(F.mask).apply(sourceTile, maskTile)
    ).as[Tile]

  /** Where the mask tile equals the mask value, replace values in the source tile with NODATA */
  def mask_by_value(sourceTile: Column, maskTile: Column, maskValue: Column): TypedColumn[Any, Tile] =
    withAlias("mask_by_value", sourceTile, maskTile, maskValue)(
      udf(F.maskByValue).apply(sourceTile, maskTile, maskValue)
    ).as[Tile]

  /** Where the mask tile DOES NOT contain NODATA, replace values in the source tile with NODATA */
  def inverse_mask(sourceTile: Column, maskTile: Column): TypedColumn[Any, Tile] =
    withAlias("inverse_mask", sourceTile, maskTile)(
      udf(F.inverseMask).apply(sourceTile, maskTile)
    ).as[Tile]

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

  /** Render Tile as ASCII string for debugging purposes. */
  @Experimental
  def render_ascii(col: Column): TypedColumn[Any, String] =
  withAlias("render_ascii", col)(
    udf[String, Tile](F.renderAscii).apply(col)
  ).as[String]

  /** Cellwise less than value comparison between two tiles. */
  def local_less(left: Column, right: Column): TypedColumn[Any, Tile] =
    withAlias("local_less", left, right)(
      udf(F.localLess).apply(left, right)
    ).as[Tile]


  /** Cellwise less than value comparison between a tile and a scalar. */
  def local_less_scalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = {
    val f = value match{
      case i: Int ⇒ F.localLessScalarInt(_: Tile, i)
      case d: Double ⇒ F.localLessScalar(_: Tile, d)
    }
    udf(f).apply(tileCol).as(s"local_less_scalar($tileCol, $value)").as[Tile]
  }

  /** Cellwise less than or equal to value comparison between a tile and a scalar. */
  def local_less_equal(left: Column, right: Column): TypedColumn[Any, Tile]  =
    withAlias("local_less_equal", left, right)(
      udf(F.localLess).apply(left, right)
    ).as[Tile]

  /** Cellwise less than or equal to value comparison between a tile and a scalar. */
  def local_less_equal_scalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = {
    val f = value match{
      case i: Int ⇒ F.localLessEqualScalarInt(_: Tile, i)
      case d: Double ⇒ F.localLessEqualScalar(_: Tile, d)
    }
    udf(f).apply(tileCol).as(s"local_less_equal_scalar($tileCol, $value)").as[Tile]
  }

  /** Cellwise greater than value comparison between two tiles. */
  def local_greater(left: Column, right: Column): TypedColumn[Any, Tile] =
    withAlias("local_greater", left, right)(
      udf(F.localGreater).apply(left, right)
    ).as[Tile]


  /** Cellwise greater than value comparison between a tile and a scalar. */
  def local_greater_scalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = {
    val f = value match{
      case i: Int ⇒ F.localGreaterScalarInt(_: Tile, i)
      case d: Double ⇒ F.localGreaterScalar(_: Tile, d)
    }
    udf(f).apply(tileCol).as(s"local_greater_scalar($tileCol, $value)").as[Tile]
  }

  /** Cellwise greater than or equal to value comparison between two tiles. */
  def local_greater_equal(left: Column, right: Column): TypedColumn[Any, Tile]  =
    withAlias("local_greater_equal", left, right)(
      udf(F.localGreaterEqual).apply(left, right)
    ).as[Tile]

  /** Cellwise greater than or equal to value comparison between a tile and a scalar. */
  def local_greater_equal_scalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = {
    val f = value match{
      case i: Int ⇒ F.localGreaterEqualScalarInt(_: Tile, i)
      case d: Double ⇒ F.localGreaterEqualScalar(_: Tile, d)
    }
    udf(f).apply(tileCol).as(s"local_greater_equal_scalar($tileCol, $value)").as[Tile]
  }

  /** Cellwise equal to value comparison between two tiles. */
  def local_equal(left: Column, right: Column): TypedColumn[Any, Tile]  =
    withAlias("local_equal", left, right)(
      udf(F.localEqual).apply(left, right)
    ).as[Tile]

  /** Cellwise equal to value comparison between a tile and a scalar. */
  def local_equal_scalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = {
    val f = value match{
      case i: Int ⇒ F.localEqualScalarInt(_: Tile, i)
      case d: Double ⇒ F.localEqualScalar(_: Tile, d)
    }
    udf(f).apply(tileCol).as(s"local_equal_scalar($tileCol, $value)").as[Tile]
  }
  /** Cellwise inequality comparison between two tiles. */
  def local_unequal(left: Column, right: Column): TypedColumn[Any, Tile]  =
    withAlias("local_unequal", left, right)(
      udf(F.localUnequal).apply(left, right)
    ).as[Tile]

  /** Cellwise inequality comparison between a tile and a scalar. */
  def local_unequal_scalar[T: Numeric](tileCol: Column, value: T): TypedColumn[Any, Tile] = {
    val f = value match{
      case i: Int ⇒ F.localUnequalScalarInt(_: Tile, i)
      case d: Double ⇒ F.localUnequalScalar(_: Tile, d)
    }
    udf(f).apply(tileCol).as(s"local_unequal_scalar($tileCol, $value)").as[Tile]
  }
}
