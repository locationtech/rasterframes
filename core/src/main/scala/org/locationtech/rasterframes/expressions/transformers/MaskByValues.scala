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
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, TernaryExpression}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.rasterframes.expressions.DynamicExtractors.intArrayExtractor
import org.locationtech.rasterframes.expressions.{RasterResult, row}
import org.locationtech.rasterframes.tileEncoder

@ExpressionDescription(
  usage =
    "_FUNC_(data, mask, maskValues) - Generate a tile with the values from `data` tile but where cells in the `mask` tile are in the `maskValues` list, replace the value with NODATA.",
  arguments = """
  Arguments:
    * target - tile to mask
    * mask - masking definition
    * maskValues - sequence of values to consider as masks candidates
        """,
  examples = """
  Examples:
    > SELECT _FUNC_(data, mask, array(1, 2, 3))
      ..."""
)
case class MaskByValues(targetTile: Expression, maskTile: Expression, maskValues: Expression)
    extends TernaryExpression with MaskExpression
    with CodegenFallback
    with RasterResult {
  override def nodeName: String = "rf_mask_by_values"

  def first: Expression = targetTile
  def second: Expression = maskTile
  def third: Expression = maskValues

  protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): Expression =
    MaskByValues(newFirst, newSecond, newThird)

  override def checkInputDataTypes(): TypeCheckResult =
    if (!intArrayExtractor.isDefinedAt(maskValues.dataType)) {
      TypeCheckFailure(s"Input type '${maskValues.dataType}' does not translate to an array<int>.")
    } else checkTileDataTypes()

  private lazy val maskValuesExtractor = intArrayExtractor(maskValues.dataType)

  override protected def nullSafeEval(targetInput: Any, maskInput: Any, maskValuesInput: Any): Any = {
    val (targetTile, targetCtx) = targetTileExtractor(row(targetInput))
    val (mask, maskCtx) = maskTileExtractor(row(maskInput))
    val maskValues: Array[Int] = maskValuesExtractor(maskValuesInput.asInstanceOf[ArrayData])

    val result = maskEval(targetTile, mask,
      { (v, m) => if (maskValues.contains(m)) NODATA else v },
      { (v, m) => if (maskValues.contains(m)) NODATA else v }
    )

    toInternalRow(result, targetCtx)
  }
}

object MaskByValues {
  def apply(dataTile: Column, maskTile: Column, maskValues: Column): TypedColumn[Any, Tile] =
    new Column(MaskByValues(dataTile.expr, maskTile.expr, maskValues.expr)).as[Tile]
}
