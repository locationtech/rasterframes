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
import geotrellis.raster.{Neighborhood, TargetCell, Tile}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.{Expression, TernaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType
import org.locationtech.rasterframes.expressions.DynamicExtractors.{neighborhoodExtractor, targetCellExtractor, tileExtractor}
import org.locationtech.rasterframes.expressions.{RasterResult, row}
import org.slf4j.LoggerFactory

trait FocalNeighborhoodOp extends TernaryExpression with RasterResult with CodegenFallback {
  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  // Tile
  def left: Expression
  // Neighborhood
  def middle: Expression
  // TargetCell
  def right: Expression

  def dataType: DataType = left.dataType
  def children: Seq[Expression] = Seq(left, middle, right)

  override def checkInputDataTypes(): TypeCheckResult =
    if (!tileExtractor.isDefinedAt(left.dataType)) TypeCheckFailure(s"Input type '${left.dataType}' does not conform to a raster type.")
    else if(!neighborhoodExtractor.isDefinedAt(middle.dataType)) TypeCheckFailure(s"Input type '${middle.dataType}' does not conform to a string Neighborhood type.")
    else if(!targetCellExtractor.isDefinedAt(right.dataType)) TypeCheckFailure(s"Input type '${right.dataType}' does not conform to a string TargetCell type.")
    else TypeCheckSuccess

  override protected def nullSafeEval(tileInput: Any, neighborhoodInput: Any, targetCellInput: Any): Any = {
    val (tile, ctx) = tileExtractor(left.dataType)(row(tileInput))
    val neighborhood = neighborhoodExtractor(middle.dataType)(neighborhoodInput)
    val target = targetCellExtractor(right.dataType)(targetCellInput)
    val result = op(extractBufferTile(tile), neighborhood, target)
    toInternalRow(result, ctx)
  }

  protected def op(child: Tile, neighborhood: Neighborhood, target: TargetCell): Tile
}

