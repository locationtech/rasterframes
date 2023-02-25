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

import geotrellis.raster.{NODATA, Tile}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckFailure
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.{Column, TypedColumn}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, TernaryExpression}
import org.locationtech.rasterframes.expressions.DynamicExtractors.intArgExtractor
import org.locationtech.rasterframes.expressions.{RasterResult, row}
import org.locationtech.rasterframes.tileEncoder


@ExpressionDescription(
  usage = "_FUNC_(target, mask, maskValue) - Generate a tile with the values from the data tile, but where cells in the masking tile DO NOT contain the masking value, replace the data value with NODATA.",
  arguments = """
  Arguments:
    * target - tile to mask
    * mask - masking definition
    * maskValue - value in the `mask` for which to mark `target` as data cells
    """,
  examples = """
  Examples:
    > SELECT _FUNC_(target, mask, maskValue);
       ..."""
)
case class InverseMaskByValue(targetTile: Expression, maskTile: Expression, maskValue: Expression)
  extends TernaryExpression with MaskExpression
    with CodegenFallback
    with RasterResult {
  override def nodeName: String = "rf_inverse_mask_by_value"

  def first: Expression = targetTile
  def second: Expression = maskTile
  def third: Expression = maskValue

  protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): Expression =
    InverseMaskByValue(newFirst, newSecond, newThird)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!intArgExtractor.isDefinedAt(maskValue.dataType)) {
      TypeCheckFailure(s"Input type '${maskValue.dataType}' isn't an integral type.")
    } else checkTileDataTypes()
  }

  private lazy val maskValueExtractor = intArgExtractor(maskValue.dataType)

  override protected def nullSafeEval(targetInput: Any, maskInput: Any, maskValueInput: Any): Any = {
    val (targetTile, targetCtx) = targetTileExtractor(row(targetInput))
    val (mask, maskCtx) = maskTileExtractor(row(maskInput))
    val maskValue = maskValueExtractor(maskValueInput).value

    val result = maskEval(targetTile, mask,
      { (v, m) => if (m != maskValue) NODATA else v },
      { (v, m) => if (m != maskValue) Double.NaN else v }
    )
    toInternalRow(result, targetCtx)
  }
}

object InverseMaskByValue {
  def apply(srcTile: Column, maskingTile: Column, maskValue: Column): TypedColumn[Any, Tile] =
    new Column(InverseMaskByValue(srcTile.expr, maskingTile.expr, maskValue.expr)).as[Tile]
}
