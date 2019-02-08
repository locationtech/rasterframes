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

import astraea.spark.rasterframes.encoders.CatalystSerializer._
import astraea.spark.rasterframes.model.TileContext
import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.{DoubleConstantNoDataCellType, Tile}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.rf.TileUDT
import org.apache.spark.sql.types.DataType

/** Expression for performing a Tile => Tile function while preserving any TileContext */
case class UnaryRasterOp(child: Expression, op: Tile => Tile, override val nodeName: String) extends OnTileExpression
  with CodegenFallback with LazyLogging {

  override def dataType: DataType = child.dataType

  override protected def eval(tile: Tile, ctx: Option[TileContext]): InternalRow = {
    implicit val tileSer = TileUDT.tileSerializer
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

  /** Convert the tile to a floating point type as needed for scalar operations. */
  @inline
  private def fpTile(t: Tile) = if (t.cellType.isFloatingPoint) t else t.convert(DoubleConstantNoDataCellType)

  def AddScalar[T: Numeric](tile: Column, value: T): Column = value match {
    case i: Int => UnaryRasterOp(tile, gt.Add(_, i), "local_add_scalar")
    case d: Double => UnaryRasterOp(tile, t => gt.Add(fpTile(t), d), "local_add_scalar")
    case o => AddScalar(tile, implicitly[Numeric[T]].toDouble(o))
  }

  def SubtractScalar[T: Numeric](tile: Column, value: T): Column = value match {
    case i: Int => UnaryRasterOp(tile, gt.Subtract(_, i), "local_subtract_scalar")
    case d: Double => UnaryRasterOp(tile, t => gt.Subtract(fpTile(t), d), "local_subtract_scalar")
    case o => SubtractScalar(tile, implicitly[Numeric[T]].toDouble(o))
  }

  def MultiplyScalar[T: Numeric](tile: Column, value: T): Column = value match {
    case i: Int => UnaryRasterOp(tile, gt.Multiply(_, i), "local_multiply_scalar")
    case d: Double => UnaryRasterOp(tile, t => gt.Multiply(fpTile(t), d), "local_multiply_scalar")
    case o => MultiplyScalar(tile, implicitly[Numeric[T]].toDouble(o))
  }

  def DivideScalar[T: Numeric](tile: Column, value: T): Column = value match {
    case i: Int => UnaryRasterOp(tile, gt.Divide(_, i), "local_divide_scalar")
    case d: Double => UnaryRasterOp(tile, t => gt.Divide(fpTile(t), d), "local_divide_scalar")
    case o => DivideScalar(tile, implicitly[Numeric[T]].toDouble(o))
  }
}

