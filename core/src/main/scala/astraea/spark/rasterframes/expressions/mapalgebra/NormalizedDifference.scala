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
import astraea.spark.rasterframes.expressions.fpTile
import astraea.spark.rasterframes._
import astraea.spark.rasterframes.expressions.BinaryRasterOp
import geotrellis.raster.Tile
import org.apache.spark.sql.{Column, TypedColumn}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback

/** Computes (left - right) / (left + right) on two tile columns. */
case class NormalizedDifference(left: Expression, right: Expression) extends BinaryRasterOp with CodegenFallback {

  override val nodeName: String = "normalized_difference"
  override protected def op(left: Tile, right: Tile): Tile = {
    val diff = fpTile(left.localSubtract(right))
    val sum = fpTile(left.localAdd(right))
    diff.localDivide(sum)
  }
}
object NormalizedDifference {
  def apply(left: Column, right: Column): TypedColumn[Any, Tile] =
    new Column(NormalizedDifference(left.expr, right.expr)).as[Tile]
}
