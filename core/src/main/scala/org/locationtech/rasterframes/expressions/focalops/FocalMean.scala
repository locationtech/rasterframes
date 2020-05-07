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

import geotrellis.raster.Tile
import geotrellis.raster.mapalgebra.focal.Neighborhood
import org.apache.spark.sql.{Column, TypedColumn}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.locationtech.rasterframes.expressions.{NullToValue, UnaryRasterOperator}

@ExpressionDescription(
  usage = "_FUNC_(tile) - ",
  arguments = """
  Arguments:
    * tile -
    * neighborhood - """,
  examples = """
  Examples:
    > SELECT  _FUNC_(tile, Square(1);
       ..."""
)
case class FocalMean(child: Expression, neighborhood: Neighborhood) extends UnaryRasterOperator with NullToValue with CodegenFallback {
  override def nodeName: String = "rf_focal_mean"
  override def na: Any = null
  override protected def op(t: Tile): Tile = t.focalMean(neighborhood)
}

object FocalMean {
  def apply(tile: Column, neighborhood: Neighborhood): TypedColumn[Any, Tile] = FocalMean(tile, neighborhood)
}
