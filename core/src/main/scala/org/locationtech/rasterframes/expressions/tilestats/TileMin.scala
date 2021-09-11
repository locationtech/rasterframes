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

package org.locationtech.rasterframes.expressions.tilestats

import org.locationtech.rasterframes.encoders.SparkBasicEncoders._
import org.locationtech.rasterframes.expressions.{NullToValue, UnaryRasterOp}
import geotrellis.raster.{Tile, isData}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.types.{DataType, DoubleType}
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.rasterframes.model.TileContext

@ExpressionDescription(
  usage = "_FUNC_(tile) - Determines the minimum cell value.",
  arguments = """
  Arguments:
    * tile - tile column to analyze""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile);
       -1"""
)
case class TileMin(child: Expression) extends UnaryRasterOp with NullToValue with CodegenFallback {
  override def nodeName: String = "rf_tile_min"
  protected def eval(tile: Tile,  ctx: Option[TileContext]): Any = TileMin.op(tile)
  def dataType: DataType = DoubleType
  def na: Any = Double.MaxValue
}
object TileMin {
  def apply(tile: Column): TypedColumn[Any, Double] = new Column(TileMin(tile.expr)).as[Double]

  /** Find the minimum cell value. */
  val op: Tile => Double = (tile: Tile) => {
    var min: Double = Double.MaxValue
    tile.foreachDouble(z => if(isData(z)) min = math.min(min, z))
    if (min == Double.MaxValue) Double.NaN
    else min
  }
}
