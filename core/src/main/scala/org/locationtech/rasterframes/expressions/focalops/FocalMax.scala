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

import geotrellis.raster.{BufferTile, Tile}
import geotrellis.raster.mapalgebra.focal.Neighborhood
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}

@ExpressionDescription(
  usage = "_FUNC_(tile, neighborhood) - Performs focalMax on tile in the neighborhood.",
  arguments = """
  Arguments:
    * tile - a tile to apply operation
    * neighborhood - a focal operation neighborhood""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile, Square(1));
       ..."""
)
case class FocalMax(child: Expression, neighborhood: Neighborhood) extends FocalNeighborhoodOp {
  override def nodeName: String = FocalMax.name
  protected def op(t: Tile): Tile = t match {
    case bt: BufferTile => bt.focalMax(neighborhood)
    case _ => t.focalMax(neighborhood)
  }
}

object FocalMax {
  def name: String = "rf_focal_max"
  def apply(tile: Column, neighborhood: Neighborhood): Column = new Column(FocalMax(tile.expr, neighborhood))
}
