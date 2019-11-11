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
import geotrellis.raster.mapalgebra.local.IfCell
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.types.{ArrayType, DataType}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.rf.TileUDT
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.expressions.DynamicExtractors._
import org.locationtech.rasterframes.expressions._

@ExpressionDescription(
  usage = "_FUNC_(tile, rhs) - In each cell of `tile`, return true if the value is in rhs.",
  arguments = """
  Arguments:
    * tile - tile column to apply abs
    * rhs - array to test against
  """,
  examples = """
  Examples:
    > SELECT  _FUNC_(tile, array(lit(33), lit(66), lit(99)));
       ..."""
)
case class IsIn(left: Expression, right: Expression) extends BinaryExpression with CodegenFallback {
  override val nodeName: String = "rf_local_is_in"

  override def dataType: DataType = left.dataType

  @transient private lazy val elementType: DataType = right.dataType.asInstanceOf[ArrayType].elementType

  override def checkInputDataTypes(): TypeCheckResult =
    if(!tileExtractor.isDefinedAt(left.dataType)) {
      TypeCheckFailure(s"Input type '${left.dataType}' does not conform to a raster type.")
    } else right.dataType match {
      case _: ArrayType ⇒ TypeCheckSuccess
      case _ ⇒ TypeCheckFailure(s"Input type '${right.dataType}' does not conform to ArrayType.")
    }

  override protected def nullSafeEval(input1: Any, input2: Any): Any = {
    implicit val tileSer = TileUDT.tileSerializer
    val (childTile, childCtx) = tileExtractor(left.dataType)(row(input1))

    val arr = input2.asInstanceOf[ArrayData].toArray[AnyRef](elementType)

    childCtx match {
      case Some(ctx) => ctx.toProjectRasterTile(op(childTile, arr)).toInternalRow
      case None => op(childTile, arr).toInternalRow
    }

  }

  protected def op(left: Tile, right: IndexedSeq[AnyRef]): Tile = {
    def fn(i: Int): Boolean = right.contains(i)
    IfCell(left, fn(_), 1, 0)
  }

}

object IsIn {
  def apply(left: Column, right: Column): Column =
    new Column(IsIn(left.expr, right.expr))

  def apply(left: Column, right: Array[Int]): Column = {
    import org.apache.spark.sql.functions.lit
    import org.apache.spark.sql.functions.array
    val arrayExpr = array(right.map(lit):_*).expr
    new Column(IsIn(left.expr, arrayExpr))
  }

}
