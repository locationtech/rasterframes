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

package org.locationtech.rasterframes.expressions.accessors

import org.locationtech.rasterframes.expressions.OnCellGridExpression
import geotrellis.raster.{CellGrid, Dimensions}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.encoders.syntax._

/**
 * Extract a raster's dimensions
 * @since 12/21/17
 */
@ExpressionDescription(
  usage = "_FUNC_(raster) - Fetches the dimensions (columns & rows) of a Tile, ProjectedRasterTile or RasterSource.",
  examples = """
    Examples:
      > SELECT _FUNC_(raster);
         ....
  """)
case class GetDimensions(child: Expression) extends OnCellGridExpression with CodegenFallback {
  override def nodeName: String = "rf_dimensions"

  def dataType = dimensionsEncoder[Int].schema

  def eval(grid: CellGrid[Int]): Any = Dimensions[Int](grid.cols, grid.rows).toInternalRow
}

object GetDimensions {
  def apply(col: Column): TypedColumn[Any, Dimensions[Int]] = new Column(new GetDimensions(col.expr)).as[Dimensions[Int]]
}
