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

package astraea.spark.rasterframes.expressions.arith
import astraea.spark.rasterframes.expressions.RasterScalarOp
import geotrellis.raster.Tile
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.functions._
import geotrellis.raster.mapalgebra.{local => gt}

case class AddScalar(left: Expression, right: Expression) extends RasterScalarOp with CodegenFallback {
  override val nodeName: String = "local_add_scalar"
  override protected def op(tile: Tile, value: Int): Tile = gt.Add(tile, value)
  override protected def op(tile: Tile, value: Double): Tile = gt.Add(tile, value)
}

object AddScalar {
  def apply(tile: Column, value: Column): Column =
    new Column(new AddScalar(tile.expr, value.expr))
  def apply[N: Numeric](tile: Column, value: N): Column =
    new Column(new AddScalar(tile.expr, lit(value).expr))
}
