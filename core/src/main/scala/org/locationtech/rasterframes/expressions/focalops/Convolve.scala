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

import com.typesafe.scalalogging.Logger
import geotrellis.raster.{BufferTile, TargetCell, Tile}
import geotrellis.raster.mapalgebra.focal.Kernel
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, TernaryExpression}
import org.apache.spark.sql.types.DataType
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.encoders._
import org.locationtech.rasterframes.encoders.syntax._
import org.locationtech.rasterframes.expressions.DynamicExtractors.{targetCellExtractor, tileExtractor}
import org.locationtech.rasterframes.expressions.{RasterResult, row}
import org.slf4j.LoggerFactory

@ExpressionDescription(
  usage = "_FUNC_(tile, kernel, target) - Performs convolve on tile in the neighborhood.",
  arguments = """
  Arguments:
    * tile - a tile to apply operation
    * kernel - a focal operation kernel
    * target - the target cells to apply focal operation: data, nodata, all""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile, kernel, 'all');
       ..."""
)
case class Convolve(left: Expression, middle: Expression, right: Expression) extends TernaryExpression with RasterResult with CodegenFallback {
  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  override def nodeName: String = Convolve.name

  def dataType: DataType = left.dataType
  val children: Seq[Expression] = Seq(left, middle, right)

  override def checkInputDataTypes(): TypeCheckResult =
    if (!tileExtractor.isDefinedAt(left.dataType)) TypeCheckFailure(s"Input type '${left.dataType}' does not conform to a raster type.")
    else if (!middle.dataType.conformsToSchema(kernelEncoder.schema)) TypeCheckFailure(s"Input type '${middle.dataType}' does not conform to a Kernel type.")
    else if (!targetCellExtractor.isDefinedAt(right.dataType)) TypeCheckFailure(s"Input type '${right.dataType}' does not conform to a TargetCell type.")
    else TypeCheckSuccess

  override protected def nullSafeEval(tileInput: Any, kernelInput: Any, targetCellInput: Any): Any = {
    val (tile, ctx) = tileExtractor(left.dataType)(row(tileInput))
    val kernel = row(kernelInput).as[Kernel]
    val target = targetCellExtractor(right.dataType)(targetCellInput)
    val result = op(extractBufferTile(tile), kernel, target)
    toInternalRow(result, ctx)
  }

  protected def op(t: Tile, kernel: Kernel, target: TargetCell): Tile = t match {
    case bt: BufferTile => bt.convolve(kernel, target = target)
    case _ => t.convolve(kernel, target = target)
  }
}

object Convolve {
  def name: String = "rf_convolve"
  def apply(tile: Column, kernel: Column, target: Column): Column = new Column(Convolve(tile.expr, kernel.expr, target.expr))
}
