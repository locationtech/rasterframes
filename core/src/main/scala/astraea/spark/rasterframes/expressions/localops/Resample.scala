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
import astraea.spark.rasterframes.expressions.BinaryLocalRasterOp
import astraea.spark.rasterframes.expressions.DynamicExtractors.tileExtractor
import geotrellis.raster.Tile
import geotrellis.raster.resample.NearestNeighbor
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, TypedColumn}

@ExpressionDescription(
  usage = "_FUNC_(tile, factor) - Resample tile to different size based on scalar factor or tile whose dimension to match. Scalar less than one will downsample tile; greater than one will upsample. Uses nearest-neighbor value.",
  arguments = """
  Arguments:
    * tile - tile
    * rhs  - scalar or tile to match dimension""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile, 2.0);
       ...
    > SELECT _FUNC_(tile1, tile2);
       ..."""
)
case class Resample(left: Expression, right: Expression) extends BinaryLocalRasterOp
  with CodegenFallback {
  override val nodeName: String = "resample"
  override protected def op(left: Tile, right: Tile): Tile = left.resample(right.cols, right.rows, NearestNeighbor)
  override protected def op(left: Tile, right: Double): Tile = left.resample((left.cols * right).toInt,
                                                                             (left.rows * right).toInt, NearestNeighbor)
  override protected def op(left: Tile, right: Int): Tile = op(left, right.toDouble)

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
}
object Resample{
  def apply(left: Column, right: Column): TypedColumn[Any, Tile] =
    new Column(Resample(left.expr, right.expr)).as[Tile]

  def apply[N: Numeric](tile: Column, value: N): TypedColumn[Any, Tile] =
    new Column(Resample(tile.expr, lit(value).expr)).as[Tile]
}
