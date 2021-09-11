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

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.locationtech.rasterframes.expressions.DynamicExtractors._
import org.locationtech.rasterframes.expressions.{RasterResult, row}
import org.slf4j.LoggerFactory

@ExpressionDescription(
  usage = "_FUNC_(tile, value) - Set the NoData value for the given tile.",
  arguments = """
  Arguments:
    * tile - left-hand-side tile
    * rhs  - a scalar value to set as the NoData value""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile, 1.5);
       ..."""
)
case class SetNoDataValue(left: Expression, right: Expression) extends BinaryExpression with RasterResult with CodegenFallback {
  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  override val nodeName: String = "rf_with_no_data"
  def dataType: DataType = left.dataType

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!tileExtractor.isDefinedAt(left.dataType)) {
      TypeCheckFailure(s"Input type '${left.dataType}' does not conform to a raster type.")
    }
    else if (!numberArgExtractor.isDefinedAt(right.dataType)) {
      TypeCheckFailure(s"Input type '${right.dataType}' does not conform to a compatible type.")
    }
    else TypeCheckSuccess
  }

  override protected def nullSafeEval(input1: Any, input2: Any): Any = {
    val (leftTile, leftCtx) = tileExtractor(left.dataType)(row(input1))

    val result = numberArgExtractor(right.dataType)(input2) match {
      case DoubleArg(d) => leftTile.withNoData(Some(d))
      case IntegerArg(i) => leftTile.withNoData(Some(i.toDouble))
    }

    toInternalRow(result, leftCtx)
  }
}

object SetNoDataValue {
  def apply(left: Column, right: Column): Column =
    new Column(SetNoDataValue(left.expr, right.expr))
  def apply[N: Numeric](tile: Column, value: N): Column =
    new Column(SetNoDataValue(tile.expr, lit(value).expr))
}
