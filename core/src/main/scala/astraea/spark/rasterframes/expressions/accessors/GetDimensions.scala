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

package astraea.spark.rasterframes.expressions.accessors

import astraea.spark.rasterframes.encoders.CatalystSerializer
import astraea.spark.rasterframes.encoders.CatalystSerializer._
import astraea.spark.rasterframes.expressions.OnCellGridExpression
import astraea.spark.rasterframes.model.TileDimensions
import geotrellis.raster.CellGrid
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback

/**
 * Extract a Tile's dimensions
 * @since 12/21/17
 */
case class GetDimensions(child: Expression) extends OnCellGridExpression
  with CodegenFallback {
  override def nodeName: String = "tile_dimensions"

  def dataType = CatalystSerializer[TileDimensions].schema

  override def eval(grid: CellGrid): Any = TileDimensions(grid.cols, grid.rows).toInternalRow
}

object GetDimensions {
  def apply(col: Column): Column =
    new Column(new GetDimensions(col.expr)).as[TileDimensions]
}
