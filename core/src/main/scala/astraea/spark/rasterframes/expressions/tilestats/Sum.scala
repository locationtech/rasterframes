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
import astraea.spark.rasterframes.encoders.SparkDefaultEncoders._
import astraea.spark.rasterframes.expressions.UnaryRasterOp
import astraea.spark.rasterframes.model.TileContext
import geotrellis.raster._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{DataType, DoubleType}
import org.apache.spark.sql.{Column, TypedColumn}

case class Sum(child: Expression) extends UnaryRasterOp with CodegenFallback {
  override def nodeName: String = "tile_sum"
  override def dataType: DataType = DoubleType
  override protected def eval(tile: Tile,  ctx: Option[TileContext]): Any = Sum.op(tile)
}

object Sum {
  def apply(tile: Column): TypedColumn[Any, Double] =
    new Column(Sum(tile.expr)).as[Double]

  def op = (tile: Tile) => {
    var sum: Double = 0.0
    tile.foreachDouble(z ⇒ if(isData(z)) sum = sum + z)
    sum
  }
}
