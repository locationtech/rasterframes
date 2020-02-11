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

package org.locationtech.rasterframes.expressions.localops

import geotrellis.raster.Tile
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.types.DataType
import org.locationtech.rasterframes.expressions.{UnaryLocalRasterOp, fpTile}


@ExpressionDescription(
  usage = "_FUNC_(tile) - Performs cell-wise exponential.",
  arguments = """
  Arguments:
    * tile - input tile""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile);
       ..."""
)
case class Exp(child: Expression) extends UnaryLocalRasterOp with CodegenFallback {
  override val nodeName: String = "rf_exp"

  override protected def op(tile: Tile): Tile = fpTile(tile).localPowValue(math.E)

  override def dataType: DataType = child.dataType
}
object Exp {
  def apply(tile: Column): Column = new Column(Exp(tile.expr))
}

@ExpressionDescription(
  usage = "_FUNC_(tile) - Compute 10 to the power of cell values.",
  arguments = """
  Arguments:
    * tile - input tile""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile);
       ..."""
)
case class Exp10(child: Expression) extends UnaryLocalRasterOp with CodegenFallback {
  override val nodeName: String = "rf_log10"

  override protected def op(tile: Tile): Tile = fpTile(tile).localPowValue(10.0)

  override def dataType: DataType = child.dataType
}
object Exp10 {
  def apply(tile: Column): Column = new Column(Exp10(tile.expr))
}

@ExpressionDescription(
  usage = "_FUNC_(tile) - Compute 2 to the power of cell values.",
  arguments = """
  Arguments:
    * tile - input tile""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile);
       ..."""
)
case class Exp2(child: Expression) extends UnaryLocalRasterOp with CodegenFallback {
  override val nodeName: String = "rf_exp2"

  override protected def op(tile: Tile): Tile = fpTile(tile).localPowValue(2.0)

  override def dataType: DataType = child.dataType
}
object Exp2{
  def apply(tile: Column): Column = new Column(Exp2(tile.expr))
}

@ExpressionDescription(
  usage = "_FUNC_(tile) - Performs cell-wise exponential, then subtract one.",
  arguments = """
  Arguments:
    * tile - input tile""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile);
       ..."""
)
case class ExpM1(child: Expression) extends UnaryLocalRasterOp with CodegenFallback {
  override val nodeName: String = "rf_expm1"

  override protected def op(tile: Tile): Tile = fpTile(tile).localPowValue(math.E).localSubtract(1.0)

  override def dataType: DataType = child.dataType
}
object ExpM1{
  def apply(tile: Column): Column = new Column(ExpM1(tile.expr))
}

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
  override protected def op(tile: Tile): Tile = fpTile(tile).localPowValue(0.5)
  override def dataType: DataType = child.dataType
}
object Sqrt {
  def apply(tile: Column): Column = new Column(Sqrt(tile.expr))
}
