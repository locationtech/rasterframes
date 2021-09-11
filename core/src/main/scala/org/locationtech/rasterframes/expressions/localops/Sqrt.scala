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

package org.locationtech.rasterframes.expressions.localops

import geotrellis.raster.Tile
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.types.DataType
import org.locationtech.rasterframes.expressions.{UnaryLocalRasterOp, fpTile}

@ExpressionDescription(
  usage = "_FUNC_(tile) - Perform cell-wise square root",
  arguments = """
  Arguments:
    * tile - input tile
  """,
  examples =
    """
    Examples:
      > SELECT _FUNC_(tile)
      ... """
)
case class Sqrt(child: Expression) extends UnaryLocalRasterOp with CodegenFallback {
  override val nodeName: String = "rf_sqrt"
  protected def op(tile: Tile): Tile = fpTile(tile).localPow(0.5)
  override def dataType: DataType = child.dataType
}
object Sqrt {
  def apply(tile: Column): Column = new Column(Sqrt(tile.expr))
}
