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
import org.locationtech.rasterframes.expressions.{UnaryRasterOp, fpTile}

@ExpressionDescription(
  usage = "_FUNC_(tile) - Performs cell-wise natural logarithm.",
  arguments = """
  Arguments:
    * tile - input tile""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile);
       ..."""
)
case class Log(child: Expression) extends UnaryRasterOp with CodegenFallback {
  override val nodeName: String = "log"

  protected def op(tile: Tile): Tile = fpTile(tile).localLog()

  override def dataType: DataType = child.dataType
  def withNewChildInternal(newChild: Expression): Expression = copy(newChild)
}
object Log {
  def apply(tile: Column): Column = new Column(Log(tile.expr))
}

@ExpressionDescription(
  usage = "_FUNC_(tile) - Performs cell-wise logarithm with base 10.",
  arguments = """
  Arguments:
    * tile - input tile""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile);
       ..."""
)
case class Log10(child: Expression) extends UnaryRasterOp with CodegenFallback {
  override val nodeName: String = "rf_log10"

  protected def op(tile: Tile): Tile = fpTile(tile).localLog10()

  override def dataType: DataType = child.dataType
  def withNewChildInternal(newChild: Expression): Expression = copy(newChild)
}
object Log10 {
  def apply(tile: Column): Column = new Column(Log10(tile.expr))
}

@ExpressionDescription(
  usage = "_FUNC_(tile) - Performs cell-wise logarithm with base 2.",
  arguments = """
  Arguments:
    * tile - input tile""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile);
       ..."""
)
case class Log2(child: Expression) extends UnaryRasterOp with CodegenFallback {
  override val nodeName: String = "rf_log2"

  protected def op(tile: Tile): Tile = fpTile(tile).localLog() / math.log(2.0)

  override def dataType: DataType = child.dataType
  def withNewChildInternal(newChild: Expression): Expression = copy(newChild)
}
object Log2 {
  def apply(tile: Column): Column = new Column(Log2(tile.expr))
}

@ExpressionDescription(
  usage = "_FUNC_(tile) - Performs natural logarithm of cell values plus one.",
  arguments = """
  Arguments:
    * tile - input tile""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile);
       ..."""
)
case class Log1p(child: Expression) extends UnaryRasterOp with CodegenFallback {
  override val nodeName: String = "rf_log1p"

  protected def op(tile: Tile): Tile = fpTile(tile).localAdd(1.0).localLog()

  override def dataType: DataType = child.dataType
  def withNewChildInternal(newChild: Expression): Expression = copy(newChild)
}
object Log1p {
  def apply(tile: Column): Column = new Column(Log1p(tile.expr))
}
