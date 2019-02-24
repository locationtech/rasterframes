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

package astraea.spark.rasterframes.expressions.mapalgebra

import astraea.spark.rasterframes.encoders.CatalystSerializer
import astraea.spark.rasterframes.encoders.CatalystSerializer._
import astraea.spark.rasterframes.expressions.row
import astraea.spark.rasterframes.model.TileContext
import astraea.spark.rasterframes.tiles.ProjectedRasterTile
import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.Tile
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression}
import org.apache.spark.sql.rf.{TileUDT, _}
import org.apache.spark.sql.types.DataType


case class BinaryRasterOp(left: Expression, right: Expression, op: (Tile, Tile) => Tile, override val nodeName: String) extends BinaryExpression
  with CodegenFallback with LazyLogging {

  override def dataType: DataType = left.dataType

  private val extractor: PartialFunction[DataType, InternalRow => (Tile, Option[TileContext])] = {
    case _: TileUDT =>
      (row: InternalRow) => (row.to[Tile](TileUDT.tileSerializer), None)
    case t if t.conformsTo(CatalystSerializer[ProjectedRasterTile].schema) =>
      (row: InternalRow) => {
        val prt = row.to[ProjectedRasterTile]
        (prt, Some(TileContext(prt)))
      }
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!extractor.isDefinedAt(left.dataType)) {
      TypeCheckFailure(s"Input type '${left.dataType}' does not conform to a raster type.")
    }
    else if (!extractor.isDefinedAt(right.dataType)) {
      TypeCheckFailure(s"Input type '${right.dataType}' does not conform to a raster type.")
    }
    else TypeCheckSuccess
  }

  override protected def nullSafeEval(input1: Any, input2: Any): Any = {
    implicit val tileSer = TileUDT.tileSerializer
    val (leftTile, leftCtx) = extractor(left.dataType)(row(input1))
    val (rightTile, rightCtx) = extractor(right.dataType)(row(input2))

    if (leftCtx.isEmpty && rightCtx.isDefined)
      logger.warn(
          s"Right-hand parameter '${right}' provided an extent and CRS, but the left-hand parameter " +
            s"'${left}' didn't have any. Because the left-hand side defines output type, the right-hand context will be lost.")

    if(leftCtx.isDefined && rightCtx.isDefined && leftCtx != rightCtx)
      logger.warn(s"Both '${left}' and '${right}' provided an extent and CRS, but they are different. Left-hand side will be used.")

    val result = op(leftTile, rightTile)

    leftCtx match {
      case Some(ctx) => ctx.toProjectRasterTile(result).toInternalRow
      case None => result.toInternalRow
    }
  }
}
object BinaryRasterOp {
  import geotrellis.raster.mapalgebra.{local => gt}

  def apply(left: Column, right: Column, op: (Tile, Tile) => Tile, nodeName: String): Column =
    new Column(BinaryRasterOp(left.expr, right.expr, op, nodeName))

  def Add(left: Expression, right: Expression)= new BinaryRasterOp(left, right, gt.Add.apply, "local_add")
  def Add(left: Column, right: Column): Column = new Column(Add(left.expr, right.expr))

  def Subtract(left: Expression, right: Expression)= new BinaryRasterOp(left, right, gt.Subtract.apply, "local_subtract")
  def Subtract(left: Column, right: Column): Column = new Column(Subtract(left.expr, right.expr))

  def Multiply(left: Expression, right: Expression)= new BinaryRasterOp(left, right, gt.Multiply.apply, "local_multiply")
  def Multiply(left: Column, right: Column): Column = new Column(Multiply(left.expr, right.expr))

  def Divide(left: Expression, right: Expression) = new BinaryRasterOp(left, right, gt.Divide.apply, "local_divide")
  def Divide(left: Column, right: Column): Column = new Column(Divide(left.expr, right.expr))

}
