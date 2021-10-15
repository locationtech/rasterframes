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
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.rf.QuinaryExpression
import org.apache.spark.sql.types.DataType
import org.locationtech.rasterframes.encoders.syntax._
import org.locationtech.rasterframes.expressions.DynamicExtractors.{DoubleArg, IntegerArg, numberArgExtractor, targetCellExtractor, tileExtractor}
import org.locationtech.rasterframes.expressions.{RasterResult, row}
import org.locationtech.rasterframes.model.TileContext
import org.slf4j.LoggerFactory

@ExpressionDescription(
  usage = "_FUNC_(tile, azimuth, altitude, zFactor, target) - Performs hillshade on tile.",
  arguments = """
  Arguments:
    * tile - a tile to apply operation
    * azimuth
    * altitude
    * zFactor
    * target - the target cells to apply focal operation: data, nodata, all""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile, azimuth, altitude, zFactor, 'all');
       ..."""
)
case class Hillshade(first: Expression, second: Expression, third: Expression, fourth: Expression, fifth: Expression) extends QuinaryExpression with RasterResult with CodegenFallback {
  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  override def nodeName: String = Hillshade.name

  def dataType: DataType = first.dataType

  val children: Seq[Expression] = Seq(first, second, third, fourth, fifth)
  val numbers: Seq[Expression] = Seq(second, third, fourth)

  override def checkInputDataTypes(): TypeCheckResult =
    if (!tileExtractor.isDefinedAt(first.dataType)) TypeCheckFailure(s"Input type '${first.dataType}' does not conform to a raster type.")
    else if (!numbers.forall(expr => numberArgExtractor.isDefinedAt(expr.dataType)))
      TypeCheckFailure(s"Input type '${second.dataType}', '${third.dataType}' or '${fourth.dataType}' do not conform to a numeric type.")
    else if(!targetCellExtractor.isDefinedAt(fifth.dataType)) TypeCheckFailure(s"Input type '${fifth.dataType}' does not conform to a string TargetCell type.")
    else TypeCheckSuccess

  override protected def nullSafeEval(tileInput: Any, azimuthInput: Any, altitudeInput: Any, zFactorInput: Any, targetCellInput: Any): Any = {
    val (tile, ctx) = tileExtractor(first.dataType)(row(tileInput))
    val List(azimuth, altitude, zFactor) =
      children
        .tail
        .zip(List(azimuthInput, altitudeInput, zFactorInput))
        .map { case (expr, datum) => numberArgExtractor(expr.dataType)(datum) match {
          case DoubleArg(value) => value
          case IntegerArg(value) => value.toDouble
        } }
    val target = targetCellExtractor(fifth.dataType)(targetCellInput)
    eval(extractBufferTile(tile), ctx, azimuth, altitude, zFactor, target)
  }

  protected def eval(tile: Tile, ctx: Option[TileContext], azimuth: Double, altitude: Double, zFactor: Double, target: TargetCell): Any = ctx match {
    case Some(ctx) => ctx.toProjectRasterTile(op(tile, ctx, azimuth, altitude, zFactor, target)).toInternalRow
    case None      => new NotImplementedError("Surface operation requires ProjectedRasterTile")
  }

  protected def op(t: Tile, ctx: TileContext, azimuth: Double, altitude: Double, zFactor: Double, target: TargetCell): Tile = t match {
    case bt: BufferTile => bt.mapTile(_.hillshade(CellSize(ctx.extent, cols = t.cols, rows = t.rows), azimuth, altitude, zFactor, target = target))
    case _ => t.hillshade(CellSize(ctx.extent, cols = t.cols, rows = t.rows), azimuth, altitude, zFactor, target = target)
  }
}

object Hillshade {
  def name: String = "rf_hillshade"
  def apply(tile: Column, azimuth: Column, altitude: Column, zFactor: Column, target: Column): Column =
    new Column(Hillshade(tile.expr, azimuth.expr, altitude.expr, zFactor.expr, target.expr))
}
