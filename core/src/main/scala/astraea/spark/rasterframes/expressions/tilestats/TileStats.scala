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

import astraea.spark.rasterframes.expressions.UnaryRasterOp
import astraea.spark.rasterframes.model.TileContext
import astraea.spark.rasterframes.stats.CellStatistics
import geotrellis.raster.Tile
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, TypedColumn}

@ExpressionDescription(
  usage = "_FUNC_(tile) - Computes per-tile descriptive statistics.",
  arguments = """
  Arguments:
    * tile - tile column to analyze""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile);
       ..."""
)
case class TileStats(child: Expression) extends UnaryRasterOp
  with CodegenFallback {
  override def nodeName: String = "tile_stats"
  override protected def eval(tile: Tile, ctx: Option[TileContext]): Any =
    TileStats.converter(TileStats.op(tile).orNull)
  override def dataType: DataType = CellStatistics.schema
}
object TileStats {
  def apply(tile: Column): TypedColumn[Any, CellStatistics] =
    new Column(TileStats(tile.expr)).as[CellStatistics]

  private lazy val converter = CatalystTypeConverters.createToCatalystConverter(CellStatistics.schema)

  /** Single tile statistics. */
  val op = (t: Tile) ⇒ CellStatistics(t)
}