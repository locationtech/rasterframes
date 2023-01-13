/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2020 Astraea, Inc.
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

import geotrellis.raster.{BufferTile, Tile}
import geotrellis.raster.mapalgebra.focal.{Neighborhood, TargetCell}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}

@ExpressionDescription(
  usage = "_FUNC_(tile, neighborhood, target) - Performs focalMoransI on tile in the neighborhood.",
  arguments = """
  Arguments:
    * tile - a tile to apply operation
    * neighborhood - a focal operation neighborhood
    * target - the target cells to apply focal operation: data, nodata, all""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile, 'square-1', 'all');
       ..."""
)
case class FocalMoransI(first: Expression, second: Expression, third: Expression) extends FocalNeighborhoodOp {
  override def nodeName: String = FocalMoransI.name
  protected def op(t: Tile, neighborhood: Neighborhood, target: TargetCell): Tile = t match {
    case bt: BufferTile => bt.tileMoransI(neighborhood, target = target)
    case _ => t.tileMoransI(neighborhood, target = target)
  }
  def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): Expression = copy(newFirst, newSecond, newThird)
}

object FocalMoransI {
  def name: String = "rf_focal_moransi"
  def apply(tile: Column, neighborhood: Column, target: Column): Column = new Column(FocalMoransI(tile.expr, neighborhood.expr, target.expr))
}
