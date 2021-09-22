/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2021 Azavea, Inc.
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

package org.locationtech.rasterframes.expressions.focalops

import geotrellis.raster.{BufferTile, CellSize, Tile}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.locationtech.rasterframes.model.TileContext

@ExpressionDescription(
  usage = "_FUNC_(tile) - Performs aspect on tile.",
  arguments = """
  Arguments:
    * tile - a tile to apply operation""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile);
       ..."""
)
case class Aspect(child: Expression) extends SurfaceOp {
  override def nodeName: String = Aspect.name
  def op(t: Tile, ctx: TileContext): Tile = t match {
    case bt: BufferTile => bt.aspect(CellSize(ctx.extent, cols = t.cols, rows = t.rows))
    case _ => t.aspect(CellSize(ctx.extent, cols = t.cols, rows = t.rows))
  }
}

object Aspect {
  def name: String = "rf_aspect"
  def apply(tile: Column): Column = new Column(Aspect(tile.expr))
}