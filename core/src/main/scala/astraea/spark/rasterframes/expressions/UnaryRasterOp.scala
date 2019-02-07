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

package astraea.spark.rasterframes.expressions

import astraea.spark.rasterframes.encoders.CatalystSerializer
import astraea.spark.rasterframes.encoders.CatalystSerializer._
import astraea.spark.rasterframes.model.TileContext
import astraea.spark.rasterframes.tiles.ProjectedRasterTile
import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.Tile
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.rf.{TileUDT, _}
import org.apache.spark.sql.types.DataType

/** Expression for performing a Tile => Tile function while preserving any TileContext */
case class UnaryRasterOp(child: Expression, op: Tile => Tile, override val nodeName: String) extends UnaryExpression
  with CodegenFallback with LazyLogging {

  override def dataType: DataType = child.dataType

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
    if (!extractor.isDefinedAt(child.dataType)) {
      TypeCheckFailure(s"Input type '${child.dataType}' does not conform to a raster type.")
    }
    else TypeCheckSuccess
  }

  override protected def nullSafeEval(input: Any): Any = {
    implicit val tileSer = TileUDT.tileSerializer
    val (tile, ctx) = extractor(child.dataType)(row(input))

    val result = op(tile)

    ctx match {
      case Some(c) => c.toProjectRasterTile(result).toInternalRow
      case None => result.toInternalRow
    }
  }
}
object UnaryRasterOp {
  import geotrellis.raster.mapalgebra.{local => gt}

  def apply(tile: Column, op: Tile => Tile, nodeName: String): Column =
    new Column(new UnaryRasterOp(tile.expr, op, nodeName))

  def AddScalar[T: Numeric](tile: Column, value: T): Column = value match {
    case i: Int => UnaryRasterOp(tile, gt.Add(_, i), "local_add_scalar")
    case d: Double => UnaryRasterOp(tile, gt.Add(_, d), "local_add_scalar")
    case o => AddScalar(tile, implicitly[Numeric[T]].toDouble(o))
  }

  def SubtractScalar[T: Numeric](tile: Column, value: T): Column = value match {
    case i: Int => UnaryRasterOp(tile, gt.Subtract(_, i), "local_subtract_scalar")
    case d: Double => UnaryRasterOp(tile, gt.Subtract(_, d), "local_subtract_scalar")
    case o => SubtractScalar(tile, implicitly[Numeric[T]].toDouble(o))
  }

  def MultiplyScalar[T: Numeric](tile: Column, value: T): Column = value match {
    case i: Int => UnaryRasterOp(tile, gt.Multiply(_, i), "local_multiply_scalar")
    case d: Double => UnaryRasterOp(tile, gt.Multiply(_, d), "local_multiply_scalar")
    case o => MultiplyScalar(tile, implicitly[Numeric[T]].toDouble(o))
  }

  def DivideScalar[T: Numeric](tile: Column, value: T): Column = value match {
    case i: Int => UnaryRasterOp(tile, gt.Divide(_, i), "local_divide_scalar")
    case d: Double => UnaryRasterOp(tile, gt.Divide(_, d), "local_divide_scalar")
    case o => DivideScalar(tile, implicitly[Numeric[T]].toDouble(o))
  }
}

