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
  usage = "_FUNC_(tile, min, max) - Rescale cell values such that the minimum is zero and the maximum is one. Other values will be linearly interpolated into the range. If specified, the `min` parameter will become the zero value and the `max` parameter will become 1. Values outside the range will be set to 0 or 1. If `min` and `max` are not specified, the tile-wise minimum and maximum are used; this can result in inconsistent values across rows in a tile column.",
  arguments = """
  Arguments:
    * tile - tile column to extract values
    * min - cell value that will become 0; cells below this are set to 0
    * max - cell value that will become 1; cells above this are set to 1
  """,
  examples = """
  Examples:
    > SELECT  _FUNC_(tile, lit(-2.2), lit(2.2))
       ..."""
)
case class Rescale(child1: Expression, child2: Expression, child3: Expression) extends TernaryExpression with CodegenFallback with Serializable {
  override val nodeName: String = "rf_rescale"

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

    val min =  doubleArgExtractor(child2.dataType)(input2).value

    val max = doubleArgExtractor(child3.dataType)(input3).value

    val result = op(childTile, min, max)

    childCtx match {
      case Some(ctx) => ctx.toProjectRasterTile(result).toInternalRow
      case None => result.toInternalRow
    }
  }

  protected def op(tile: Tile, min: Double, max: Double): Tile = {
    // convert tile to float if not
    // clamp to min and max
    // "normalize" linearlly rescale to 0,1 range
    tile.convert(FloatConstantNoDataCellType)
        .localMin(max) // See Clip
        .localMax(min)
        .normalize(min, max, 0.0, 1.0)
  }

}

object Rescale {
  def apply(tile: Column, min: Column, max: Column): Column =
    new Column(Rescale(tile.expr, min.expr, max.expr))

  def apply(tile: Column, min: Double, max: Double): Column =
    new Column(Rescale(tile.expr, lit(min).expr, lit(max).expr))

  def apply(tile: Column): Column = {
    val stats = TileStats(tile)
    val min = stats.getField("min").expr
    val max = stats.getField("max").expr

    new Column(Rescale(tile.expr, min, max))
  }
}


