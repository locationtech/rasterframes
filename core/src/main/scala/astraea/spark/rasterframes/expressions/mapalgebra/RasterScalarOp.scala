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
import astraea.spark.rasterframes.expressions.DynamicExtractors.tileExtractor
import astraea.spark.rasterframes.expressions.row
import geotrellis.raster.{DoubleConstantNoDataCellType, Tile}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.BinaryExpression
import org.apache.spark.sql.rf.TileUDT
import org.apache.spark.sql.types.{DataType, NumericType}
import astraea.spark.rasterframes.encoders.CatalystSerializer._

/** Expression for performing a (Tile, Numaric) => Tile function while preserving any TileContext */
trait RasterScalarOp extends BinaryExpression {
  protected val TileType = new TileUDT()

  override def dataType: DataType = left.dataType

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!tileExtractor.isDefinedAt(left.dataType)) {
      TypeCheckFailure(s"Input type '${left.dataType}' does not contain a tile type.")
    }
    else right.dataType match {
      case _: NumericType => TypeCheckSuccess
      case _ =>
        TypeCheckFailure(s"Input type '${right.dataType}' does not contain a numeric type.")
    }
  }

  protected def op(tile: Tile, value: Int): Tile
  protected def op(tile: Tile, value: Double): Tile

  /** Convert the tile to a floating point type as needed for scalar operations. */
  @inline
  private def fpTile(t: Tile) = if (t.cellType.isFloatingPoint) t else t.convert(DoubleConstantNoDataCellType)

  override protected def nullSafeEval(tileIn: Any, valueIn: Any): Any = {
    implicit val tileSer = TileUDT.tileSerializer

    val (tile, ctx) = tileExtractor(left.dataType)(row(tileIn))
    val result: Tile = valueIn match {
      case i: Int => op(tile, i)
      case d: Double => op(fpTile(tile), d)
      case f: Float => op(fpTile(tile), f.toDouble)
      case b: Byte => op(tile, b.toInt)
      case s: Short => op(tile, s.toInt)
    }
    ctx match {
      case Some(c) => c.toProjectRasterTile(result).toInternalRow
      case None => result.toInternalRow
    }
  }
}
