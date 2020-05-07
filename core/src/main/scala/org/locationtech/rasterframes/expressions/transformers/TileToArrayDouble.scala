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

package org.locationtech.rasterframes.expressions.transformers

import org.locationtech.rasterframes.expressions.UnaryRasterFunction
import geotrellis.raster.Tile
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{DataType, DataTypes, DoubleType}
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.rasterframes.model.TileContext

@ExpressionDescription(
  usage = "_FUNC_(tile) - Coverts the contents of the given tile to an array of double floating-point values",
  arguments = """
  Arguments:
    * tile - tile to convert"""
)
case class TileToArrayDouble(child: Expression) extends UnaryRasterFunction with CodegenFallback {
  override def nodeName: String = "rf_tile_to_array_double"
  override def dataType: DataType = DataTypes.createArrayType(DoubleType, false)
  override protected def eval(tile: Tile, ctx: Option[TileContext]): Any = {
    ArrayData.toArrayData(tile.toArrayDouble())
  }
}
object TileToArrayDouble {
  import org.locationtech.rasterframes.encoders.StandardEncoders.PrimitiveEncoders.arrayEnc
  def apply(tile: Column): TypedColumn[Any, Array[Double]] =
    new Column(TileToArrayDouble(tile.expr)).as[Array[Double]]
}
