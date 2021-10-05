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

import geotrellis.raster.{BufferTile, CellSize, TargetCell, Tile}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription}
import org.locationtech.rasterframes.expressions.{RasterResult, row}
import org.locationtech.rasterframes.encoders.syntax._
import org.locationtech.rasterframes.expressions.DynamicExtractors._
import org.locationtech.rasterframes.model.TileContext
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}

@ExpressionDescription(
  usage = "_FUNC_(tile, target) - Performs aspect on tile.",
  arguments = """
  Arguments:
    * tile - a tile to apply operation
    * target - the target cells to apply focal operation: data, nodata, all""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile, 'all');
       ..."""
)
case class Aspect(left: Expression, right: Expression) extends BinaryExpression with RasterResult with CodegenFallback {
  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  def dataType: DataType = left.dataType

  override def checkInputDataTypes(): TypeCheckResult =
    if (!tileExtractor.isDefinedAt(left.dataType)) TypeCheckFailure(s"Input type '${left.dataType}' does not conform to a raster type.")
    else if(!targetCellExtractor.isDefinedAt(right.dataType)) TypeCheckFailure(s"Input type '${right.dataType}' does not conform to a string TargetCell type.")
    else TypeCheckSuccess

  override protected def nullSafeEval(tileInput: Any, targetCellInput: Any): Any = {
    val (tile, ctx) = tileExtractor(left.dataType)(row(tileInput))
    val target = targetCellExtractor(right.dataType)(targetCellInput)
    eval(extractBufferTile(tile), ctx, target)
  }

  protected def eval(tile: Tile, ctx: Option[TileContext], target: TargetCell): Any = ctx match {
    case Some(ctx) => ctx.toProjectRasterTile(op(tile, ctx, target)).toInternalRow
    case None => new NotImplementedError("Surface operation requires ProjectedRasterTile")
  }

  override def nodeName: String = Aspect.name

  def op(t: Tile, ctx: TileContext, target: TargetCell): Tile = t match {
    case bt: BufferTile => bt.aspect(CellSize(ctx.extent, cols = t.cols, rows = t.rows), target = target)
    case _ => t.aspect(CellSize(ctx.extent, cols = t.cols, rows = t.rows), target = target)
  }
}

object Aspect {
  def name: String = "rf_aspect"
  def apply(tile: Column, target: Column): Column = new Column(Aspect(tile.expr, target.expr))
}
