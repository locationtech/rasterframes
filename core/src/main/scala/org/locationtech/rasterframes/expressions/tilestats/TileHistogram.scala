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

import org.locationtech.rasterframes.stats.CellHistogram
import geotrellis.raster.Tile
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.rasterframes.expressions.UnaryRasterFunction
import org.locationtech.rasterframes.model.TileContext

@ExpressionDescription(
  usage = "_FUNC_(tile) - Computes per-tile histogram.",
  arguments = """
  Arguments:
    * tile - tile column to analyze""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile);
       ..."""
)
case class TileHistogram(child: Expression) extends UnaryRasterFunction with CodegenFallback {
  override def nodeName: String = "rf_tile_histogram"
  protected def eval(tile: Tile, ctx: Option[TileContext]): Any =
    TileHistogram.converter(TileHistogram.op(tile))
  def dataType: DataType = CellHistogram.schema
}

object TileHistogram {
  def apply(tile: Column): TypedColumn[Any, CellHistogram] = new Column(TileHistogram(tile.expr)).as[CellHistogram]

  private lazy val converter = CatalystTypeConverters.createToCatalystConverter(CellHistogram.schema)

  /** Single tile histogram. */
  val op: Tile => CellHistogram = (t: Tile) => CellHistogram(t)
}