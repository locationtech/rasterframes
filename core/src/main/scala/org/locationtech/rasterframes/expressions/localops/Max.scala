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
import org.locationtech.rasterframes.expressions.BinaryLocalRasterOp

@ExpressionDescription(
  usage = "_FUNC_(tile, rhs) - Performs cell-wise maximum two tiles or a tile and a scalar.",
  arguments = """
  Arguments:
    * tile - left-hand-side tile
    * rhs  - a tile or scalar value""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile, 1.5);
       ...
    > SELECT _FUNC_(tile1, tile2);
       ..."""
)
case class Max(left: Expression, right:Expression) extends BinaryLocalRasterOp with CodegenFallback {

  override val nodeName = "rf_local_max"
  protected def op(left: Tile, right: Tile): Tile = left.localMax(right)
  protected def op(left: Tile, right: Double): Tile = left.localMax(right)
  protected def op(left: Tile, right: Int): Tile = left.localMax(right)
}
object Max {
  def apply(left: Column, right: Column): Column = new Column(Max(left.expr, right.expr))
  def apply[N: Numeric](tile: Column, value: N): Column = new Column(Max(tile.expr, lit(value).expr))
}
