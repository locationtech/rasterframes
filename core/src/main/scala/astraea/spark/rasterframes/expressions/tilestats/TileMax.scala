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

package astraea.spark.rasterframes.expressions.tilestats

import astraea.spark.rasterframes.expressions.{NullToValue, UnaryRasterOp}
import astraea.spark.rasterframes.model.TileContext
import geotrellis.raster.{Tile, isData}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.types.{DataType, DoubleType}
import org.apache.spark.sql.{Column, TypedColumn}

@ExpressionDescription(
  usage = "_FUNC_(tile) - Determines the maximum cell value.",
  arguments = """
  Arguments:
    * tile - tile column to analyze""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile);
       1"""
)
case class TileMax(child: Expression) extends UnaryRasterOp
  with NullToValue with CodegenFallback {
  override def nodeName: String = "tile_max"
  override protected def eval(tile: Tile,  ctx: Option[TileContext]): Any = TileMax.op(tile)
  override def dataType: DataType = DoubleType
  override def na: Any = Double.MinValue
}
object TileMax {
  import astraea.spark.rasterframes.encoders.StandardEncoders.PrimitiveEncoders.doubleEnc

  def apply(tile: Column): TypedColumn[Any, Double] =
    new Column(TileMax(tile.expr)).as[Double]

  /** Find the maximum cell value. */
  val op = (tile: Tile) ⇒ {
    var max: Double = Double.MinValue
    tile.foreachDouble(z ⇒ if(isData(z)) max = math.max(max, z))
    if (max == Double.MinValue) Double.NaN
    else max
  }
}