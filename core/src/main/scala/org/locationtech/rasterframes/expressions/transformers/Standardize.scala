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

package org.locationtech.rasterframes.expressions.transformers

import geotrellis.raster.{FloatConstantNoDataCellType, Tile}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, TernaryExpression}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.rf.TileUDT
import org.apache.spark.sql.types.DataType
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.expressions.DynamicExtractors._
import org.locationtech.rasterframes.expressions._
import org.locationtech.rasterframes.expressions.tilestats.TileStats

@ExpressionDescription(
  usage = "_FUNC_(tile, mean, stddev) - Standardize cell values such that the mean is zero and the standard deviation is one. If specified, the `mean` and `stddev` are applied to all tiles in the column.  If not specified, each tile will be standardized according to the statistics of its cell values; this can result in inconsistent values across rows in a tile column.",
  arguments = """
  Arguments:
    * tile - tile column to extract values
    * mean - value to mean-center the cell values around
    * stddev - standard deviation to apply in standardization
  """,
  examples = """
  Examples:
    > SELECT  _FUNC_(tile, lit(4.0), lit(2.2))
       ..."""
)
case class Standardize(child1: Expression, child2: Expression, child3: Expression) extends TernaryExpression with CodegenFallback with Serializable {
  override val nodeName: String = "rf_standardize"

  override def children: Seq[Expression] = Seq(child1, child2, child3)

  override def dataType: DataType = child1.dataType

  override def checkInputDataTypes(): TypeCheckResult =
    if(!tileExtractor.isDefinedAt(child1.dataType)) {
      TypeCheckFailure(s"Input type '${child1.dataType}' does not conform to a raster type.")
    } else if (!doubleArgExtractor.isDefinedAt(child2.dataType)) {
      TypeCheckFailure(s"Input type '${child2.dataType}' isn't floating point type.")
    } else if (!doubleArgExtractor.isDefinedAt(child3.dataType)) {
      TypeCheckFailure(s"Input type '${child3.dataType}' isn't floating point type." )
    } else TypeCheckSuccess


  override protected def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
    implicit val tileSer = TileUDT.tileSerializer
    val (childTile, childCtx) = tileExtractor(child1.dataType)(row(input1))

    val mean = doubleArgExtractor(child2.dataType)(input2).value

    val stdDev = doubleArgExtractor(child3.dataType)(input3).value

    childCtx match {
      case Some(ctx) => ctx.toProjectRasterTile(op(childTile, mean, stdDev)).toInternalRow
      case None => op(childTile, mean, stdDev).toInternalRow
    }
  }

  protected def op(tile: Tile, mean: Double, stdDev: Double): Tile =
    tile.convert(FloatConstantNoDataCellType)
        .localSubtract(mean)
        .localDivide(stdDev)

}
object Standardize {
  def apply(tile: Column, mean: Column, stdDev: Column): Column =
    new Column(Standardize(tile.expr, mean.expr, stdDev.expr))

  def apply(tile: Column, mean: Double, stdDev: Double): Column =
    new Column(Standardize(tile.expr, lit(mean).expr, lit(stdDev).expr))

  def apply(tile: Column): Column = {
    import org.apache.spark.sql.functions.sqrt
    val stats = TileStats(tile)
    val mean = stats.getField("mean").expr
    val stdDev = sqrt(stats.getField("variance")).expr

    new Column(Standardize(tile.expr, mean, stdDev))
  }
}


