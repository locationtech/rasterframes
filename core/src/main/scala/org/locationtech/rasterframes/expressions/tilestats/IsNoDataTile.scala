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
import org.locationtech.rasterframes.expressions.{NullToValue, UnaryRasterFunction}
import geotrellis.raster._
import org.apache.spark.sql.{Column, TypedColumn}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.types.{BooleanType, DataType}
import org.locationtech.rasterframes.model.TileContext

@ExpressionDescription(
  usage = "_FUNC_(tile) - Produces `true` if all the cells in a given tile are no-data",
  arguments = """
  Arguments:
    * tile - tile column to analyze""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile);
       false"""
)
case class IsNoDataTile(child: Expression) extends UnaryRasterFunction
  with CodegenFallback with NullToValue {
  override def nodeName: String = "rf_is_no_data_tile"
  def na: Any = true
  def dataType: DataType = BooleanType
  protected def eval(tile: Tile, ctx: Option[TileContext]): Any = tile.isNoDataTile
  def withNewChildInternal(newChild: Expression): Expression = copy(newChild)
}
object IsNoDataTile {
  def apply(tile: Column): TypedColumn[Any, Boolean] = new Column(IsNoDataTile(tile.expr)).as[Boolean]
}
