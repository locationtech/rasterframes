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

package org.locationtech.rasterframes.expressions.localops

import geotrellis.raster.Tile
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.functions.lit
import org.locationtech.rasterframes.expressions.BinaryRasterFunction

@ExpressionDescription(
  usage = "_FUNC_(lhs, rhs) - Performs cell-wise less-than-or-equal (<=) test between two tiles.",
  arguments = """
  Arguments:
    * lhs - first tile argument
    * rhs - second tile argument""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile1, tile2);
       ..."""
)
case class LessEqual(left: Expression, right: Expression) extends BinaryRasterFunction with CodegenFallback  {
  override val nodeName: String = "rf_local_less_equal"
  protected def op(left: Tile, right: Tile): Tile = left.localLessOrEqual(right)
  protected def op(left: Tile, right: Double): Tile = left.localLessOrEqual(right)
  protected def op(left: Tile, right: Int): Tile = left.localLessOrEqual(right)
}
object LessEqual {
  def apply(left: Column, right: Column): Column = new Column(LessEqual(left.expr, right.expr))

  def apply[N: Numeric](tile: Column, value: N): Column = new Column(LessEqual(tile.expr, lit(value).expr))
}
