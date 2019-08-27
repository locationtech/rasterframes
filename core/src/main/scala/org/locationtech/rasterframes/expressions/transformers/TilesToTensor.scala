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

import geotrellis.raster.Tile
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataTypes.createArrayType
import org.apache.spark.sql.types.{DataType, DataTypes, DoubleType}
import org.apache.spark.sql.{Column, Encoder, TypedColumn}
import org.locationtech.rasterframes.expressions.DynamicExtractors.tileExtractor
import org.locationtech.rasterframes.expressions.row
import TilesToTensor.TileHasMatrix

@ExpressionDescription(
  usage = "_FUNC_(tile1, tile2, ...) - Coverts the columns of tiles into a 3rd order tensor of size (t, r, c) " +
    "where 't' is the number of columns, 'r' is the number of rows in each tile, and 'c' is the number of " +
    "columns in each tile.",
  arguments = """
  Arguments:
    * tile1, tile2, ... - var args list of tile columns"""
)
case class TilesToTensor(children: Seq[Expression]) extends Expression with CodegenFallback {
  override def checkInputDataTypes(): TypeCheckResult = {
    val baddies = children.find(c => !tileExtractor.isDefinedAt(c.dataType))
    baddies.map(b =>
      TypeCheckFailure(s"input $b (type ${b.dataType} does not conform to a raster type.")
    ).getOrElse(TypeCheckSuccess)
  }

  override def nodeName: String = "rf_tensor"

  override def nullable: Boolean = false

  override def dataType: DataType =
    createArrayType(
      createArrayType(
        createArrayType(
          DoubleType,
          false),
        false),
      false)

  override def eval(input: InternalRow): Any = {
    val tiles = children.map(c => tileExtractor(c.dataType)(row(c.eval(input)))).map(_._1)
    val tars = tiles.map(t =>
      ArrayData.toArrayData(t.toMatrix.map(row => ArrayData.toArrayData(row)))
    )
      .toArray
    ArrayData.toArrayData(tars)
  }
}

object TilesToTensor {
  import scala.reflect.runtime.universe._
  implicit def arrayArrayEnc[T: TypeTag]: Encoder[Array[Array[T]]] = ExpressionEncoder()

  def apply(tiles: Seq[Column]): TypedColumn[Any, Array[Array[Array[Double]]]] =
    new Column(TilesToTensor(tiles.map(_.expr))).as[Array[Array[Array[Double]]]]

  implicit class TileHasMatrix(val tile: Tile) extends AnyVal {
    import spire.syntax.cfor._
    /** Converts tile into a two dimensional array of size (rows x cols) */
    def toMatrix: Array[Array[Double]] = {
      val retval = Array.ofDim[Double](tile.rows, tile.cols)
      cfor(0)(_ < tile.rows, _ + 1) { row =>
        cfor(0)(_ < tile.cols, _ + 1) { col =>
          retval(row)(col) = tile.getDouble(col, row)
        }
      }
      retval
    }
  }
}
