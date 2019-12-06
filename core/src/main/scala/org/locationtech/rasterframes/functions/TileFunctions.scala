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
import geotrellis.raster.render.ColorRamp
import geotrellis.raster.{CellType, Tile}
import org.apache.spark.sql.functions.{lit, udf}
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.jts.geom.Geometry
import org.locationtech.rasterframes.expressions.TileAssembler
import org.locationtech.rasterframes.expressions.accessors._
import org.locationtech.rasterframes.expressions.generators._
import org.locationtech.rasterframes.expressions.localops._
import org.locationtech.rasterframes.expressions.tilestats._
import org.locationtech.rasterframes.expressions.transformers.RenderPNG.{RenderColorRampPNG, RenderCompositePNG}
import org.locationtech.rasterframes.expressions.transformers._
import org.locationtech.rasterframes.stats._
import org.locationtech.rasterframes.tiles.ProjectedRasterTile
import org.locationtech.rasterframes.util.{withTypedAlias, ColorRampNames, _}
import org.locationtech.rasterframes.{encoders, singlebandTileEncoder, functions => F}

/** Functions associated with creating and transforming tiles, including tile-wise statistics and rendering. */
trait TileFunctions {

  /** Extracts the tile from a ProjectedRasterTile, or passes through a Tile. */
  def rf_tile(col: Column): TypedColumn[Any, Tile] = RealizeTile(col)

  /** Flattens Tile into a double array. */
  def rf_tile_to_array_double(col: Column): TypedColumn[Any, Array[Double]] =
    TileToArrayDouble(col)

  /** Flattens Tile into an integer array. */
  def rf_tile_to_array_int(col: Column): TypedColumn[Any, Array[Double]] =
    TileToArrayDouble(col)

  /** Convert array in `arrayCol` into a Tile of dimensions `cols` and `rows`*/
  def rf_array_to_tile(arrayCol: Column, cols: Int, rows: Int): TypedColumn[Any, Tile] = withTypedAlias("rf_array_to_tile")(
    udf[Tile, AnyRef](F.arrayToTile(cols, rows)).apply(arrayCol).as[Tile]
  )

  /** Create a Tile from a column of cell data with location indexes and preform cell conversion. */
  def rf_assemble_tile(
    columnIndex: Column,
    rowIndex: Column,
    cellData: Column,
    tileCols: Int,
    tileRows: Int,
    ct: CellType): TypedColumn[Any, Tile] =
    rf_convert_cell_type(TileAssembler(columnIndex, rowIndex, cellData, lit(tileCols), lit(tileRows)), ct)
      .as(cellData.columnName)
      .as[Tile](singlebandTileEncoder)

  /** Create a Tile from a column of cell data with location indexes and perform cell conversion. */
  def rf_assemble_tile(columnIndex: Column, rowIndex: Column, cellData: Column, tileCols: Int, tileRows: Int): TypedColumn[Any, Tile] =
    TileAssembler(columnIndex, rowIndex, cellData, lit(tileCols), lit(tileRows))

  /** Create a Tile from  a column of cell data with location indexes. */
  def rf_assemble_tile(
    columnIndex: Column,
    rowIndex: Column,
    cellData: Column,
    tileCols: Column,
    tileRows: Column): TypedColumn[Any, Tile] =
    TileAssembler(columnIndex, rowIndex, cellData, tileCols, tileRows)

  /** Extract the Tile's cell type */
  def rf_cell_type(col: Column): TypedColumn[Any, CellType] = GetCellType(col)

  /** Change the Tile's cell type */
  def rf_convert_cell_type(col: Column, cellType: CellType): Column = SetCellType(col, cellType)

  /** Change the Tile's cell type */
  def rf_convert_cell_type(col: Column, cellTypeName: String): Column = SetCellType(col, cellTypeName)

  /** Change the Tile's cell type */
  def rf_convert_cell_type(col: Column, cellType: Column): Column = SetCellType(col, cellType)

  /** Change the interpretation of the Tile's cell values according to specified CellType */
  def rf_interpret_cell_type_as(col: Column, cellType: CellType): Column = InterpretAs(col, cellType)

  /** Change the interpretation of the Tile's cell values according to specified CellType */
  def rf_interpret_cell_type_as(col: Column, cellTypeName: String): Column = InterpretAs(col, cellTypeName)

  /** Change the interpretation of the Tile's cell values according to specified CellType */
  def rf_interpret_cell_type_as(col: Column, cellType: Column): Column = InterpretAs(col, cellType)

  /** Resample tile to different size based on scalar factor or tile whose dimension to match. Scalar less
   * than one will downsample tile; greater than one will upsample. Uses nearest-neighbor. */
  def rf_resample[T: Numeric](tileCol: Column, factorValue: T) = Resample(tileCol, factorValue)

  /** Resample tile to different size based on scalar factor or tile whose dimension to match. Scalar less
   * than one will downsample tile; greater than one will upsample. Uses nearest-neighbor. */
  def rf_resample(tileCol: Column, factorCol: Column) = Resample(tileCol, factorCol)

  /** Assign a `NoData` value to the tile column. */
  def rf_with_no_data(col: Column, nodata: Double): Column = SetNoDataValue(col, nodata)

  /** Assign a `NoData` value to the tile column. */
  def rf_with_no_data(col: Column, nodata: Int): Column = SetNoDataValue(col, nodata)

  /** Assign a `NoData` value to the tile column. */
  def rf_with_no_data(col: Column, nodata: Column): Column = SetNoDataValue(col, nodata)

  /** Constructor for tile column with a single cell value. */
  def rf_make_constant_tile(value: Number, cols: Int, rows: Int, cellType: CellType): TypedColumn[Any, Tile] =
    rf_make_constant_tile(value, cols, rows, cellType.name)

  /** Constructor for tile column with a single cell value. */
  def rf_make_constant_tile(value: Number, cols: Int, rows: Int, cellTypeName: String): TypedColumn[Any, Tile] = {
    val constTile = udf(() => F.makeConstantTile(value, cols, rows, cellTypeName))
    withTypedAlias(s"rf_make_constant_tile($value, $cols, $rows, $cellTypeName)")(constTile.apply())
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

  /** Construct a `proj_raster` structure from individual CRS, Extent, and Tile columns. */
  def rf_proj_raster(tile: Column, extent: Column, crs: Column): TypedColumn[Any, ProjectedRasterTile] =
    CreateProjectedRaster(tile, extent, crs)

  /** Compute the Tile-wise mean */
  def rf_tile_mean(col: Column): TypedColumn[Any, Double] = TileMean(col)

  /** Compute the Tile-wise sum */
  def rf_tile_sum(col: Column): TypedColumn[Any, Double] = Sum(col)

  /** Compute the minimum cell value in tile. */
  def rf_tile_min(col: Column): TypedColumn[Any, Double] = TileMin(col)

  /** Compute the maximum cell value in tile. */
  def rf_tile_max(col: Column): TypedColumn[Any, Double] = TileMax(col)

  /** Compute TileHistogram of Tile values. */
  def rf_tile_histogram(col: Column): TypedColumn[Any, CellHistogram] = TileHistogram(col)

  /** Compute statistics of Tile values. */
  def rf_tile_stats(col: Column): TypedColumn[Any, CellStatistics] = TileStats(col)

  /** Counts the number of non-NoData cells per Tile. */
  def rf_data_cells(tile: Column): TypedColumn[Any, Long] = DataCells(tile)

  /** Counts the number of NoData cells per Tile. */
  def rf_no_data_cells(tile: Column): TypedColumn[Any, Long] = NoDataCells(tile)

  /** Returns true if all cells in the tile are NoData.*/
  def rf_is_no_data_tile(tile: Column): TypedColumn[Any, Boolean] = IsNoDataTile(tile)

  /** Returns true if any cells in the tile are true (non-zero and not NoData). */
  def rf_exists(tile: Column): TypedColumn[Any, Boolean] = Exists(tile)

  /** Returns true if all cells in the tile are true (non-zero and not NoData). */
  def rf_for_all(tile: Column): TypedColumn[Any, Boolean] = ForAll(tile)

  /** Create a tile where cells in the grid defined by cols, rows, and bounds are filled with the given value. */
  def rf_rasterize(geometry: Column, bounds: Column, value: Column, cols: Int, rows: Int): TypedColumn[Any, Tile] =
    withTypedAlias("rf_rasterize", geometry)(
      udf(F.rasterize(_: Geometry, _: Geometry, _: Int, cols, rows)).apply(geometry, bounds, value)
    )

  /** Create a tile where cells in the grid defined by cols, rows, and bounds are filled with the given value. */
  def rf_rasterize(geometry: Column, bounds: Column, value: Column, cols: Column, rows: Column): TypedColumn[Any, Tile] =
    withTypedAlias("rf_rasterize", geometry)(
      udf(F.rasterize).apply(geometry, bounds, value, cols, rows)
    )

  /** Render Tile as ASCII string, for debugging purposes. */
  def rf_render_ascii(tile: Column): TypedColumn[Any, String] = DebugRender.RenderAscii(tile)

  /** Render Tile cell values as numeric values, for debugging purposes. */
  def rf_render_matrix(tile: Column): TypedColumn[Any, String] = DebugRender.RenderMatrix(tile)

  /** Converts tiles in a column into PNG encoded byte array, using given ColorRamp to assign values to colors. */
  def rf_render_png(tile: Column, colors: ColorRamp): TypedColumn[Any, Array[Byte]] = RenderColorRampPNG(tile, colors)

  /** Converts tiles in a column into PNG encoded byte array, using given ColorRamp to assign values to colors. */
  def rf_render_png(tile: Column, colorRampName: String): TypedColumn[Any, Array[Byte]] = {
    colorRampName match {
      case ColorRampNames(ramp) => RenderColorRampPNG(tile, ramp)
      case _ => throw new IllegalArgumentException(
        s"Provided color ramp name '${colorRampName}' does not match one of " + ColorRampNames().mkString("\n\t", "\n\t", "\n")
      )
    }
  }

  /** Converts columns of tiles representing RGB channels into a PNG encoded byte array. */
  def rf_render_png(red: Column, green: Column, blue: Column): TypedColumn[Any, Array[Byte]] = RenderCompositePNG(red, green, blue)

  /** Converts columns of tiles representing RGB channels into a single RGB packaged tile. */
  def rf_rgb_composite(red: Column, green: Column, blue: Column): Column = RGBComposite(red, green, blue)

  /** Create a row for each cell in Tile. */
  def rf_explode_tiles(cols: Column*): Column = rf_explode_tiles_sample(1.0, None, cols: _*)

  /** Create a row for each cell in Tile with random sampling and optional seed. */
  def rf_explode_tiles_sample(sampleFraction: Double, seed: Option[Long], cols: Column*): Column =
    ExplodeTiles(sampleFraction, seed, cols)

  /** Create a row for each cell in Tile with random sampling (no seed). */
  def rf_explode_tiles_sample(sampleFraction: Double, cols: Column*): Column = ExplodeTiles(sampleFraction, None, cols)
}

object TileFunctions extends TileFunctions
