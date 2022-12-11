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

import geotrellis.raster.{NODATA, Tile, d2i}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.{Column, TypedColumn}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, TernaryExpression}
import org.apache.spark.sql.types.{DataType}
import org.locationtech.rasterframes.expressions.DynamicExtractors.{intArgExtractor, tileExtractor}
import org.locationtech.rasterframes.expressions.{RasterResult, row}
import org.locationtech.rasterframes.tileEncoder


@ExpressionDescription(
  usage = "_FUNC_(target, mask, maskValue) - Generate a tile with the values from the data tile, but where cells in the masking tile contain the masking value, replace the data value with NODATA.",
  arguments = """
  Arguments:
    * target - tile to mask
    * mask - masking definition
    * maskValue - pixel value to consider as mask location when found in mask tile
    """,
  examples = """
  Examples:
    > SELECT _FUNC_(target, mask, maskValue);
       ..."""
)
case class MaskByValue(dataTile: Expression, maskTile: Expression, maskValue: Expression)
  extends TernaryExpression
    with CodegenFallback
    with RasterResult {
  override def nodeName: String = "rf_mask_by_value"

  def dataType: DataType = dataTile.dataType
  def first: Expression = dataTile
  def second: Expression = maskTile
  def third: Expression = maskValue

  protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): Expression =
    MaskByValue(newFirst, newSecond, newThird)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!tileExtractor.isDefinedAt(dataTile.dataType)) {
      TypeCheckFailure(s"Input type '${dataTile.dataType}' does not conform to a raster type.")
    } else if (!tileExtractor.isDefinedAt(maskTile.dataType)) {
      TypeCheckFailure(s"Input type '${maskTile.dataType}' does not conform to a raster type.")
    } else if (!intArgExtractor.isDefinedAt(maskValue.dataType)) {
      TypeCheckFailure(s"Input type '${maskValue.dataType}' isn't an integral type.")
    } else TypeCheckSuccess
  }

  private lazy val dataTileExtractor = tileExtractor(dataTile.dataType)
  private lazy val maskTileExtractor = tileExtractor(maskTile.dataType)
  private lazy val maskValueExtractor = intArgExtractor(maskValue.dataType)

  override protected def nullSafeEval(targetInput: Any, maskInput: Any, maskValueInput: Any): Any = {
    val (targetTile, targetCtx) = dataTileExtractor(row(targetInput))
    val (mask, maskCtx) = maskTileExtractor(row(maskInput))
    val maskValue = maskValueExtractor(maskValueInput).value

    val result = targetTile.dualCombine(mask)
      { (v, m) => if (m == maskValue) NODATA else v }
      { (v, m) => if (d2i(m) == maskValue) NODATA else v }
    toInternalRow(result, targetCtx)
  }
}

object MaskByValue {
  def apply(srcTile: Column, maskingTile: Column, maskValue: Column): TypedColumn[Any, Tile] =
    new Column(MaskByValue(srcTile.expr, maskingTile.expr, maskValue.expr)).as[Tile]
}
