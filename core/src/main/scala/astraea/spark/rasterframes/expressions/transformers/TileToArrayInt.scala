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

package astraea.spark.rasterframes.expressions.transformers

import astraea.spark.rasterframes.expressions.UnaryRasterOp
import astraea.spark.rasterframes.model.TileContext
import geotrellis.raster.Tile
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{DataType, DataTypes, DoubleType, IntegerType}
import org.apache.spark.sql.{Column, TypedColumn}

@ExpressionDescription(
  usage = "_FUNC_(tile) - Coverts the contents of the given tile to an array of integer values",
  arguments = """
  Arguments:
    * tile - tile to convert"""
)
case class TileToArrayInt(child: Expression) extends UnaryRasterOp with CodegenFallback {
  override def nodeName: String = "tile_to_array_int"
  override def dataType: DataType = DataTypes.createArrayType(IntegerType, false)
  override protected def eval(tile: Tile, ctx: Option[TileContext]): Any = {
    ArrayData.toArrayData(tile.toArray())
  }
}
object TileToArrayInt {
  import astraea.spark.rasterframes.encoders.StandardEncoders.PrimitiveEncoders.arrayEnc
  def apply(tile: Column): TypedColumn[Any, Array[Int]] =
    new Column(TileToArrayInt(tile.expr)).as[Array[Int]]
}
