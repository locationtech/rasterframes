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

package astraea.spark.rasterframes.expressions.mapalgebra
import astraea.spark.rasterframes._
import geotrellis.raster.Tile
import org.apache.spark.sql.{Column, TypedColumn}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.functions.lit

/** Perform cell-wise multiplication by a scalar value. */
case class MultiplyScalar (left: Expression, right: Expression) extends RasterScalarOp with CodegenFallback {
  override val nodeName: String = "local_multiply_scalar"
  override protected def op(tile: Tile, value: Int): Tile = tile.localMultiply(value)
  override protected def op(tile: Tile, value: Double): Tile = tile.localMultiply(value)
}
object MultiplyScalar {
  def apply(tile: Column, value: Column): TypedColumn[Any, Tile] =
    new Column(new MultiplyScalar(tile.expr, value.expr)).as[Tile]
  def apply[N: Numeric](tile: Column, value: N): TypedColumn[Any, Tile] =
    new Column(new MultiplyScalar(tile.expr, lit(value).expr)).as[Tile]
}
