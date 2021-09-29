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
import geotrellis.raster.{BufferTile, CellSize, Tile}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription}
import org.apache.spark.sql.types.DataType
import org.locationtech.rasterframes.encoders.syntax._
import org.locationtech.rasterframes.expressions.DynamicExtractors.{DoubleArg, IntegerArg, numberArgExtractor, tileExtractor}
import org.locationtech.rasterframes.expressions.{RasterResult, row}
import org.locationtech.rasterframes.model.TileContext
import org.slf4j.LoggerFactory

@ExpressionDescription(
  usage = "_FUNC_(tile, zFactor) - Performs slope on tile.",
  arguments = """
  Arguments:
    * tile - a tile to apply operation
    * zFactor - a slope operation zFactor""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile, 0.2);
       ..."""
)
case class Slope(left: Expression, right: Expression) extends BinaryExpression with RasterResult with CodegenFallback {
  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  override def nodeName: String = Slope.name

  def dataType: DataType = left.dataType

  override def checkInputDataTypes(): TypeCheckResult =
    if (!tileExtractor.isDefinedAt(left.dataType)) TypeCheckFailure(s"Input type '${left.dataType}' does not conform to a raster type.")
    else if (!numberArgExtractor.isDefinedAt(right.dataType)) {
      TypeCheckFailure(s"Input type '${right.dataType}' does not conform to a numeric type.")
    } else TypeCheckSuccess

  override protected def nullSafeEval(tileInput: Any, zFactorInput: Any): Any = {
    val (tile, ctx) = tileExtractor(left.dataType)(row(tileInput))
    val zFactor = numberArgExtractor(right.dataType)(zFactorInput) match {
      case DoubleArg(value)  => value
      case IntegerArg(value) => value.toDouble
    }
    eval(extractBufferTile(tile), ctx, zFactor)
  }
  protected def eval(tile: Tile, ctx: Option[TileContext], zFactor: Double): Any = ctx match {
    case Some(ctx) => ctx.toProjectRasterTile(op(tile, ctx, zFactor)).toInternalRow
    case None      => new NotImplementedError("Surface operation requires ProjectedRasterTile")
  }

  protected def op(t: Tile, ctx: TileContext, zFactor: Double): Tile = t match {
    case bt: BufferTile => bt.slope(CellSize(ctx.extent, cols = t.cols, rows = t.rows), zFactor)
    case _ => t.slope(CellSize(ctx.extent, cols = t.cols, rows = t.rows), zFactor)
  }
}

object Slope {
  def name: String = "rf_slope"
  def apply(tile: Column, zFactor: Column): Column = new Column(Slope(tile.expr, zFactor.expr))
}
