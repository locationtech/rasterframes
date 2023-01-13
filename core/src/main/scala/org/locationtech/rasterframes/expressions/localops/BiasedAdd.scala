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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.functions.lit
import org.locationtech.rasterframes.expressions.BinaryRasterFunction
import org.locationtech.rasterframes.expressions.DynamicExtractors.tileExtractor
import org.locationtech.rasterframes.util.DataBiasedOp

@ExpressionDescription(
  usage = "_FUNC_(tile, rhs) - Performs cell-wise addition between two tiles or a tile and a scalar. " +
    "Unlike a regular 'add', this considers `<data> + <nodata> = <data>.",
  arguments = """
  Arguments:
    * tile - left-hand-side tile
    * rhs  - a tile or scalar value to add to each cell""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile, 1.5);
       ...
    > SELECT _FUNC_(tile1, tile2);
       ..."""
)
case class BiasedAdd(left: Expression, right: Expression) extends BinaryRasterFunction with CodegenFallback {
  override val nodeName: String = "rf_local_biased_add"
  protected def op(left: Tile, right: Tile): Tile = DataBiasedOp.BiasedAdd(left, right)
  protected def op(left: Tile, right: Double): Tile = DataBiasedOp.BiasedAdd(left, right)
  protected def op(left: Tile, right: Int): Tile = DataBiasedOp.BiasedAdd(left, right)

  override def eval(input: InternalRow): Any = {
    if(input == null) null
    else {
      val l = left.eval(input)
      val r = right.eval(input)
      if (l == null && r == null) null
      else if (l == null) r
      else if (r == null && tileExtractor.isDefinedAt(right.dataType)) l
      else if (r == null) null
      else nullSafeEval(l, r)
    }
  }

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = copy(newLeft, newRight)
}
object BiasedAdd {
  def apply(left: Column, right: Column): Column = new Column(BiasedAdd(left.expr, right.expr))

  def apply[N: Numeric](tile: Column, value: N): Column = new Column(BiasedAdd(tile.expr, lit(value).expr))
}
