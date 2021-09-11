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

import geotrellis.raster.CellType
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.encoders._
import org.locationtech.rasterframes.encoders.syntax._
import org.locationtech.rasterframes.expressions.DynamicExtractors
import org.locationtech.rasterframes.expressions.{RasterResult, row}

@ExpressionDescription(
  usage = "_FUNC_(tile, value) - Change the interpretation of the Tile's cell values according to specified CellType",
  arguments = """
  Arguments:
    * tile - left-hand-side tile
    * rhs  - a cell type definition""",
  examples = """
  Examples:
    > SELECT _FUNC_(tile, 'int16ud0');
       ..."""
)
case class InterpretAs(tile: Expression, cellType: Expression) extends BinaryExpression with RasterResult with CodegenFallback {
  def left: Expression = tile
  def right: Expression = cellType
  override def nodeName: String = "rf_interpret_cell_type_as"
  def dataType: DataType = left.dataType

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!DynamicExtractors.tileExtractor.isDefinedAt(left.dataType))
      TypeCheckFailure(s"Input type '${left.dataType}' does not conform to a raster type.")
    else
      right.dataType match {
        case StringType => TypeCheckSuccess
        case t if t.conformsToSchema(cellTypeEncoder.schema) => TypeCheckSuccess
        case _ =>
          TypeCheckFailure(s"Expected CellType but received '${right.dataType.simpleString}'")
      }
  }

  private def toCellType(datum: Any): CellType = {
    right.dataType match {
      case StringType =>
        val text = datum.asInstanceOf[UTF8String].toString
        CellType.fromName(text)
      case st if st.conformsToSchema(cellTypeEncoder.schema) => row(datum).as[CellType]
    }
  }

  override protected def nullSafeEval(tileInput: Any, ctInput: Any): InternalRow = {
    val (tile, ctx) = DynamicExtractors.tileExtractor(left.dataType)(row(tileInput))
    val ct = toCellType(ctInput)
    val result = tile.interpretAs(ct)
    toInternalRow(result, ctx)
  }
}

object InterpretAs{
  def apply(tile: Column, cellType: CellType): Column = new Column(new InterpretAs(tile.expr, lit(cellType.name).expr))
  def apply(tile: Column, cellType: String): Column = new Column(new InterpretAs(tile.expr, lit(cellType).expr))
  def apply(tile: Column, cellType: Column): Column = new Column(new InterpretAs(tile.expr, cellType.expr))
}
