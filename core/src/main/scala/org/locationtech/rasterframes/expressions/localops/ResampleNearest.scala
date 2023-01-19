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
import geotrellis.raster.resample._
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DataType
import org.locationtech.rasterframes.expressions.{RasterResult, row}
import org.locationtech.rasterframes.expressions.DynamicExtractors._


@ExpressionDescription(
  usage =
    "_FUNC_(tile, factor) - Resample tile to different size based on scalar factor or tile whose dimension to match. Scalar less than one will downsample tile; greater than one will upsample. Uses nearest-neighbor value.",
  arguments = """
  Arguments:
    * tile - tile
    * rhs  - scalar or tile to match dimension""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile, 2.0);
       ...
    > SELECT _FUNC_(tile1, tile2);
       ..."""
)
case class ResampleNearest(tile: Expression, factor: Expression) extends BinaryExpression with RasterResult with CodegenFallback {
  override val nodeName: String = "rf_resample_nearest"
  def dataType: DataType = tile.dataType
  def left: Expression = tile
  def right: Expression = factor

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!tileExtractor.isDefinedAt(tile.dataType))
      TypeCheckFailure(s"Input type '${tile.dataType}' does not conform to a raster type.")
    else if (!tileOrNumberExtractor.isDefinedAt(factor.dataType))
      TypeCheckFailure(s"Input type '${factor.dataType}' does not conform to a compatible type.")
    else
      TypeCheckSuccess
  }

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    val (leftTile, leftCtx) = tileExtractor(tile.dataType)(row(input1))
    val ton = tileOrNumberExtractor(factor.dataType)(input2)

    val result: Tile = Resample.op(leftTile, ton, NearestNeighbor)
    toInternalRow(result, leftCtx)
  }

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    ResampleNearest(newLeft, newRight)
}

object ResampleNearest {
  def apply(tile: Column, target: Column): Column =
    new Column(ResampleNearest(tile.expr, target.expr))

  def apply[N: Numeric](tile: Column, value: N): Column =
    new Column(ResampleNearest(tile.expr, lit(value).expr))
}
