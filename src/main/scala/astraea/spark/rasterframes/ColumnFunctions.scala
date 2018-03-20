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

import astraea.spark.rasterframes.expressions.ExplodeTileExpression
import astraea.spark.rasterframes.functions.{CellCountAggregateFunction, CellMeanAggregateFunction, CellStatsAggregateFunction}
import astraea.spark.rasterframes.{functions ⇒ F}
import geotrellis.raster.histogram.Histogram
import geotrellis.raster.mapalgebra.local.LocalTileBinaryOp
import geotrellis.raster.{CellType, Tile}
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.gt._
import org.apache.spark.sql._

import scala.reflect.runtime.universe._

/**
 * UDFs for working with Tiles in Spark DataFrames.
 *
 * @author sfitch
 * @since 4/3/17
 */
trait ColumnFunctions {
  private implicit def arrayEnc[T: TypeTag]: Encoder[Array[T]] = ExpressionEncoder()
  private implicit def genEnc[T: TypeTag]: Encoder[T] = ExpressionEncoder()

  // format: off
  /** Create a row for each cell in Tile. */
  @Experimental
  def explodeTiles(cols: Column*): Column = explodeTileSample(1.0, cols: _*)

  /** Create a row for each cell in Tile with random sampling. */
  @Experimental
  def explodeTileSample(sampleFraction: Double, cols: Column*): Column = {
    val exploder = ExplodeTileExpression(sampleFraction, cols.map(_.expr))
    // Hack to grab the first two non-cell columns, containing the column and row indexes
    val metaNames = exploder.elementSchema.fieldNames.take(2)
    val colNames = cols.map(_.columnName)
    new Column(exploder).as(metaNames ++ colNames)
  }

  /** Query the number of (cols, rows) in a Tile. */
  @Experimental
  def tileDimensions(col: Column): Column = expressions.Dimensions(col.expr).asColumn

  /** Flattens Tile into an array. A numeric type parameter is required*/
  @Experimental
  def tileToArray[T: HasCellType: TypeTag](col: Column): TypedColumn[Any, Array[T]] = withAlias("tileToArray", col)(
    udf[Array[T], Tile](F.tileToArray).apply(col)
  ).as[Array[T]]

  @Experimental
  /** Convert array in `arrayCol` into a Tile of dimensions `cols` and `rows`*/
  def arrayToTile(arrayCol: Column, cols: Int, rows: Int) = withAlias("arrayToTile", arrayCol)(
    udf[Tile, AnyRef](F.arrayToTile(cols, rows)).apply(arrayCol)
  )

  /** Create a Tile from  */
  @Experimental
  def assembleTile(columnIndex: Column, rowIndex: Column, cellData: Column, cols: Int, rows: Int, ct: CellType): TypedColumn[Any, Tile] = {
    F.assembleTile(cols, rows, ct)(columnIndex, rowIndex, cellData)
  }.as(cellData.columnName).as[Tile]

  /** Extract the Tile's cell type */
  @Experimental
  def cellType(col: Column): TypedColumn[Any, String] =
    expressions.CellType(col.expr).asColumn.as[String]

  /** Assign a `NoData` value to the Tiles. */
  def withNoData(col: Column, nodata: Double) = withAlias("withNoData", col)(
    udf[Tile, Tile](F.withNoData(nodata)).apply(col)
  ).as[Tile]

  /**  Compute the full column aggregate floating point histogram. */
  @Experimental
  def aggHistogram(col: Column): TypedColumn[Any, Histogram[Double]] =
  withAlias("histogram", col)(
    F.aggHistogram(col)
  ).as[Histogram[Double]]

  /** Compute the full column aggregate floating point statistics. */
  @Experimental
  def aggStats(col: Column): TypedColumn[Any, Statistics] = withAlias("aggStats", col)(
    F.aggStats(col)
  ).as[Statistics]

  /** Computes the column aggregate mean. */
  @Experimental
  def aggMean(col: Column) = CellMeanAggregateFunction(col.expr)
    .toAggregateExpression().asColumn
    .as[Double]

  /** Computes the number of non-NoData cells in a column. */
  @Experimental
  def aggDataCells(col: Column) = CellCountAggregateFunction(true, col.expr)
    .toAggregateExpression().asColumn
    .as[Long]

  /** Computes the number of NoData cells in a column. */
  @Experimental
  def aggNoDataCells(col: Column) = CellCountAggregateFunction(false, col.expr)
    .toAggregateExpression().asColumn
    .as[Long]

  /** Compute the Tile-wise mean */
  @Experimental
  def tileMean(col: Column): TypedColumn[Any, Double] =
  withAlias("tileMean", col)(
    udf[Double, Tile](F.tileMean).apply(col)
  ).as[Double]

  /** Compute the Tile-wise sum */
  @Experimental
  def tileSum(col: Column): TypedColumn[Any, Double] =
  withAlias("tileSum", col)(
    udf[Double, Tile](F.tileSum).apply(col)
  ).as[Double]

  /** Compute the minimum cell value in tile. */
  @Experimental
  def tileMin(col: Column): TypedColumn[Any, Double] =
  withAlias("tileMin", col)(
    udf[Double, Tile](F.tileMin).apply(col)
  ).as[Double]

  /** Compute the maximum cell value in tile. */
  @Experimental
  def tileMax(col: Column): TypedColumn[Any, Double] =
  withAlias("tileMax", col)(
    udf[Double, Tile](F.tileMax).apply(col)
  ).as[Double]

  /** Compute TileHistogram of Tile values. */
  @Experimental
  def tileHistogram(col: Column): TypedColumn[Any, Histogram[Double]] =
  withAlias("tileHistogram", col)(
    udf[Histogram[Double], Tile](F.tileHistogram).apply(col)
  ).as[Histogram[Double]]

  /** Compute statistics of Tile values. */
  @Experimental
  def tileStats(col: Column): TypedColumn[Any, Statistics] =
  withAlias("tileStats", col)(
    udf[Statistics, Tile](F.tileStats).apply(col)
  ).as[Statistics]

  /** Counts the number of non-NoData cells per Tile. */
  @Experimental
  def dataCells(tile: Column): TypedColumn[Any, Long] =
    withAlias("dataCells", tile)(
      udf(F.dataCells).apply(tile)
    ).as[Long]

  /** Counts the number of NoData cells per Tile. */
  @Experimental
  def noDataCells(tile: Column): TypedColumn[Any, Long] =
    withAlias("nodataCells", tile)(
      udf(F.nodataCells).apply(tile)
    ).as[Long]

  /** Compute cell-local aggregate descriptive statistics for a column of Tiles. */
  @Experimental
  def localAggStats(col: Column): Column =
  withAlias("localAggStats", col)(
    F.localAggStats(col)
  )

  /** Compute the cell-wise/local max operation between Tiles in a column. */
  @Experimental
  def localAggMax(col: Column): TypedColumn[Any, Tile] =
  withAlias("localAggMax", col)(
    F.localAggMax(col)
  ).as[Tile]

  /** Compute the cellwise/local min operation between Tiles in a column. */
  @Experimental
  def localAggMin(col: Column): TypedColumn[Any, Tile] =
  withAlias("localAggMin", col)(
    F.localAggMin(col)
  ).as[Tile]

  /** Compute the cellwise/local mean operation between Tiles in a column. */
  @Experimental
  def localAggMean(col: Column): TypedColumn[Any, Tile] =
  withAlias("localAggMean", col)(
    F.localAggMean(col)
  ).as[Tile]

  /** Compute the cellwise/local count of non-NoData cells for all Tiles in a column. */
  @Experimental
  def localAggDataCells(col: Column): TypedColumn[Any, Tile] =
  withAlias("localCount", col)(
    F.localAggCount(col)
  ).as[Tile]

  /** Compute the cellwise/local count of NoData cells for all Tiles in a column. */
  @Experimental
  def localAggNoDataCells(col: Column): TypedColumn[Any, Tile] =
  withAlias("localCount", col)(
    F.localAggCount(col)
  ).as[Tile]

  /** Cellwise addition between two Tiles. */
  @Experimental
  def localAdd(left: Column, right: Column): TypedColumn[Any, Tile] =
  withAlias("localAdd", left, right)(
    udf(F.localAdd).apply(left, right)
  ).as[Tile]

  /** Cellwise subtraction between two Tiles. */
  @Experimental
  def localSubtract(left: Column, right: Column): TypedColumn[Any, Tile] =
  withAlias("localSubtract", left, right)(
    udf(F.localSubtract).apply(left, right)
  ).as[Tile]

  /** Cellwise multiplication between two Tiles. */
  @Experimental
  def localMultiply(left: Column, right: Column): TypedColumn[Any, Tile] =
  withAlias("localMultiply", left, right)(
    udf(F.localMultiply).apply(left, right)
  ).as[Tile]

  /** Cellwise division between two Tiles. */
  @Experimental
  def localDivide(left: Column, right: Column): TypedColumn[Any, Tile] =
  withAlias("localDivide", left, right)(
    udf(F.localDivide).apply(left, right)
  ).as[Tile]

  /** Perform an arbitrary GeoTrellis `LocalTileBinaryOp` between two Tile columns. */
  @Experimental
  def localAlgebra(op: LocalTileBinaryOp, left: Column, right: Column):
  TypedColumn[Any, Tile] =
    withAlias(opName(op), left, right)(
      udf[Tile, Tile, Tile](op.apply).apply(left, right)
    ).as[Tile]

  /** Render Tile as ASCII string for debugging purposes. */
  @Experimental
  def renderAscii(col: Column): TypedColumn[Any, String] =
  withAlias("renderAscii", col)(
    udf[String, Tile](F.renderAscii).apply(col)
  ).as[String]

  // --------------------------------------------------------------------------------------------
  // -- Private APIs below --
  // --------------------------------------------------------------------------------------------
  /** Tags output column with a nicer name. */
  private[rasterframes] def withAlias(name: String, inputs: Column*)(output: Column) = {
    val paramNames = inputs.map(_.columnName).mkString(",")
    output.as(s"$name($paramNames)")
  }

  private[rasterframes] def opName(op: LocalTileBinaryOp) =
    op.getClass.getSimpleName.replace("$", "").toLowerCase
}
