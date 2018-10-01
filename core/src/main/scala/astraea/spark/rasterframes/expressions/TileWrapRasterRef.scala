/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea, Inc.
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

package astraea.spark.rasterframes.expressions

import geotrellis.raster.Tile
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, UnaryExpression}
import org.apache.spark.sql.rf._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, TypedColumn}

/**
 * Extracts tile represented by a RasterRef.
 *
 * @since 9/5/18
 */
case class TileWrapRasterRef(child: Expression) extends UnaryExpression
  with CodegenFallback with ExpectsInputTypes {

  override def dataType: DataType = TileUDT

  override def nodeName: String = "tile_wrap_raster_ref"

  override protected def nullSafeEval(input: Any): Any = {
    val ref = RasterRefUDT.deserialize(input)
    TileUDT.serialize(ref.tile)
  }

  override def inputTypes = Seq(new RasterRefUDT)
}

object TileWrapRasterRef {
  import astraea.spark.rasterframes.encoders.StandardEncoders._
  def apply(rasterRef: Column): TypedColumn[Any, Tile] =
    new TileWrapRasterRef(rasterRef.expr).asColumn.as[Tile]
}
