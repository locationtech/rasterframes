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

import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.Tile
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.rf.TileUDT
import org.apache.spark.sql.types._
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.expressions.DynamicExtractors._
import org.locationtech.rasterframes.expressions.{fpTile, row}

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
case class SetNoDataValue(left: Expression, right: Expression) extends BinaryExpression with CodegenFallback with LazyLogging {

  override val nodeName: String = "rf_set_nodata"
  override def dataType: DataType = left.dataType

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
    implicit val tileSer = TileUDT.tileSerializer
    val (leftTile, leftCtx) = tileExtractor(left.dataType)(row(input1))
    val result = numberArgExtractor(right.dataType)(input2) match {
      case DoubleArg(d) => op(fpTile(leftTile), d)
      case IntegerArg(i) => op(leftTile, i)
    }

    leftCtx match {
      case Some(ctx) => ctx.toProjectRasterTile(result).toInternalRow
      case None => result.toInternalRow
    }
  }

  protected def op(left: Tile, right: Double): Tile = left.withNoData(Some(right))
  protected def op(left: Tile, right: Int): Tile = left.withNoData(Some(right))
}
object SetNoDataValue {
  def apply(left: Column, right: Column): Column =
    new Column(SetNoDataValue(left.expr, right.expr))
  def apply[N: Numeric](tile: Column, value: N): Column =
    new Column(SetNoDataValue(tile.expr, lit(value).expr))
}
