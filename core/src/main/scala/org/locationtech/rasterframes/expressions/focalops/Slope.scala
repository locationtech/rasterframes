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
import geotrellis.raster.mapalgebra.focal.TargetCell
import geotrellis.raster.{BufferTile, CellSize, Tile}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, TernaryExpression}
import org.apache.spark.sql.types.DataType
import org.locationtech.rasterframes.encoders.syntax._
import org.locationtech.rasterframes.expressions.DynamicExtractors.{DoubleArg, IntegerArg, numberArgExtractor, targetCellExtractor, tileExtractor}
import org.locationtech.rasterframes.expressions.{RasterResult, row}
import org.locationtech.rasterframes.model.TileContext
import org.slf4j.LoggerFactory

@ExpressionDescription(
  usage = "_FUNC_(tile, zFactor, middle) - Performs slope on tile.",
  arguments = """
  Arguments:
    * tile - a tile to apply operation
    * zFactor - a slope operation zFactor
    * target - the target cells to apply focal operation: data, nodata, all""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile, 0.2, 'all');
       ..."""
)
case class Slope(first: Expression, second: Expression, third: Expression) extends TernaryExpression with RasterResult with CodegenFallback {
  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  override def nodeName: String = Slope.name

  def dataType: DataType = first.dataType

  override def checkInputDataTypes(): TypeCheckResult =
    if (!tileExtractor.isDefinedAt(first.dataType)) TypeCheckFailure(s"Input type '${first.dataType}' does not conform to a raster type.")
    else if (!numberArgExtractor.isDefinedAt(second.dataType)) TypeCheckFailure(s"Input type '${second.dataType}' does not conform to a numeric type.")
    else if (!targetCellExtractor.isDefinedAt(third.dataType)) TypeCheckFailure(s"Input type '${third.dataType}' does not conform to a TargetCell type.")
    else TypeCheckSuccess

  override protected def nullSafeEval(tileInput: Any, zFactorInput: Any, targetCellInput: Any): Any = {
    val (tile, ctx) = tileExtractor(first.dataType)(row(tileInput))
    val zFactor = numberArgExtractor(second.dataType)(zFactorInput) match {
      case DoubleArg(value)  => value
      case IntegerArg(value) => value.toDouble
    }
    val target = targetCellExtractor(third.dataType)(targetCellInput)
    eval(extractBufferTile(tile), ctx, zFactor, target)
  }
  protected def eval(tile: Tile, ctx: Option[TileContext], zFactor: Double, target: TargetCell): Any = ctx match {
    case Some(ctx) => ctx.toProjectRasterTile(op(tile, ctx, zFactor, target)).toInternalRow
    case None      => new NotImplementedError("Surface operation requires ProjectedRasterTile")
  }

  protected def op(t: Tile, ctx: TileContext, zFactor: Double, target: TargetCell): Tile = t match {
    case bt: BufferTile => bt.slope(CellSize(ctx.extent, cols = t.cols, rows = t.rows), zFactor, target = target)
    case _ => t.slope(CellSize(ctx.extent, cols = t.cols, rows = t.rows), zFactor, target = target)
  }

  override protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): Expression = copy(newFirst, newSecond, newThird)
}

object Slope {
  def name: String = "rf_slope"
  def apply(tile: Column, zFactor: Column, target: Column): Column = new Column(Slope(tile.expr, zFactor.expr, target.expr))
}
