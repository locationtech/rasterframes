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

package astraea.spark.rasterframes.expressions.localops

import astraea.spark.rasterframes._
import astraea.spark.rasterframes.expressions.UnaryRasterOp
import astraea.spark.rasterframes.model.TileContext
import geotrellis.raster.Tile
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, TypedColumn}

@ExpressionDescription(
  usage = "_FUNC_(tile, base) - Performs cell-wise logarithm.",
  arguments = """
  Arguments:
    * tile - input tile
    * base  - base for which to compute logarithm """,
  examples = """
  Examples:
    > SELECT _FUNC_(tile);
       ...
    > SELECT _FUNC_(tile, 10);
       ..."""
)
case class Log(left: Expression, right: Expression) extends BinaryExpression with CodegenFallback {
  override val nodeName: String = "log"
  protected def op(left: Tile, right: Double): Tile = left.localLog() / math.log(right)
  protected def op(left: Tile, right: Int): Tile = op(left, right.toDouble)

  override def dataType: DataType = left.dataType
}
object Log {
  def apply(tile: Column): TypedColumn[Any, Tile] =
    new Column(Log(tile.expr, lit(math.E).expr)).as[Tile]
  def apply[N: Numeric](tile: Column, value: N): TypedColumn[Any, Tile] =
    new Column(Log(tile.expr, lit(value).expr)).as[Tile]
  def apply(tile: Column, value: Column): TypedColumn[Any, Tile] =
    new Column(Log(tile.expr, value.expr)).as[Tile]
}

