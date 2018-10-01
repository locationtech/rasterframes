/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea, Inc.
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

package astraea.spark.rasterframes.expressions

import geotrellis.raster.{CellType, Tile}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, Literal}
import org.apache.spark.sql.rf._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, TypedColumn}
import astraea.spark.rasterframes.encoders.StandardEncoders._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Change the CellType of a Tile
 *
 * @since 9/11/18
 */
case class SetCellType(tile: Expression, cellType: Expression) extends BinaryExpression with CodegenFallback {
  def left = tile
  def right = cellType
  override def nodeName: String = "set_cell_type"
  override def dataType: DataType = new TileUDT()

  private val ctEnc = cellTypeEncoder

  override def checkInputDataTypes(): TypeCheckResult = {
    RequiresTile.check(tile) match {
      case TypeCheckSuccess ⇒
        right.dataType match {
          case StringType ⇒ TypeCheckSuccess
          case st: StructType if st == ctEnc.schema ⇒ TypeCheckSuccess
          case _ ⇒ TypeCheckFailure(
            s"Expected CellType but received '${right.dataType.simpleString}'"
          )
        }
      case o ⇒ o
    }
  }

  private def toCellType(datum: Any): CellType = {
    right.dataType match {
      case StringType ⇒
        val text = datum.asInstanceOf[UTF8String].toString
        CellType.fromName(text)
      case st: StructType if st == cellTypeEncoder.schema ⇒
        ctEnc.decode(row(datum))
    }
  }

  override protected def nullSafeEval(left: Any, right: Any) = {
    val t = TileUDT.decode(row(left))
    val ct = toCellType(right)
    TileUDT.encode(t.convert(ct))
  }
}

object SetCellType {

  def apply(tile: Column, cellType: CellType): TypedColumn[Any, Tile] =
    new SetCellType(tile.expr, lit(cellType.name).expr).asColumn.as[Tile]
  def apply(tile: Column, cellType: String): TypedColumn[Any, Tile] =
    new SetCellType(tile.expr, lit(cellType).expr).asColumn.as[Tile]

}
