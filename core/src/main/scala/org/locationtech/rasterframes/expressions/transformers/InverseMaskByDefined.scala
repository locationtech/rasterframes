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

package org.locationtech.rasterframes.expressions.transformers

import geotrellis.raster.{NODATA, Tile, isNoData}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.{Column, TypedColumn}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription}
import org.apache.spark.sql.types.DataType
import org.locationtech.rasterframes.expressions.DynamicExtractors.tileExtractor
import org.locationtech.rasterframes.expressions.{RasterResult, row}
import org.locationtech.rasterframes.tileEncoder


@ExpressionDescription(
  usage = "_FUNC_(target, mask) - Generate a tile with the values from the data tile, but where cells in the masking tile DO NOT contain NODATA, replace the data value with NODATA",
  arguments = """
  Arguments:
    * target - tile to mask
    * mask - masking definition""",
  examples = """
  Examples:
    > SELECT _FUNC_(target, mask);
       ..."""
)
case class InverseMaskByDefined(targetTile: Expression, maskTile: Expression)
  extends BinaryExpression
    with CodegenFallback
    with RasterResult {
  override def nodeName: String = "rf_inverse_mask"

  def dataType: DataType = targetTile.dataType
  def left: Expression = targetTile
  def right: Expression = maskTile

  protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    InverseMaskByDefined(newLeft, newRight)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!tileExtractor.isDefinedAt(targetTile.dataType)) {
      TypeCheckFailure(s"Input type '${targetTile.dataType}' does not conform to a raster type.")
    } else if (!tileExtractor.isDefinedAt(maskTile.dataType)) {
      TypeCheckFailure(s"Input type '${maskTile.dataType}' does not conform to a raster type.")
    } else TypeCheckSuccess
  }

  private lazy val targetTileExtractor = tileExtractor(targetTile.dataType)
  private lazy val maskTileExtractor = tileExtractor(maskTile.dataType)

  override protected def nullSafeEval(targetInput: Any, maskInput: Any): Any = {
    val (targetTile, targetCtx) = targetTileExtractor(row(targetInput))
    val (mask, maskCtx) = maskTileExtractor(row(maskInput))

    val result = targetTile.dualCombine(mask)
      { (v, m) => if (isNoData(m)) v else NODATA }
      { (v, m) => if (isNoData(m)) v else NODATA }
    toInternalRow(result, targetCtx)
  }
}

object InverseMaskByDefined {
  def apply(srcTile: Column, maskingTile: Column): TypedColumn[Any, Tile] =
    new Column(InverseMaskByDefined(srcTile.expr, maskingTile.expr)).as[Tile]
}
