/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Astraea, Inc.
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
 */

package astraea.spark.rasterframes.functions

import geotrellis.raster.{DataType ⇒ _, _}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.gt.types.TileUDT
import org.apache.spark.sql.types._

import scala.collection.mutable

/**
 * Aggregator for reassembling tiles from from exploded form
 *
 * @author sfitch 
 * @since 9/24/17
 */
case class TileAssemblerFunction(cols: Int, rows: Int, ct: CellType) extends UserDefinedAggregateFunction {
  def inputSchema: StructType = StructType(Seq(
    StructField("columnIndex", IntegerType, false),
    StructField("rowIndex", IntegerType, false),
    StructField("cellValues", DoubleType, false)
  ))

  def bufferSchema: StructType = StructType(Seq(
    StructField("cells", DataTypes.createArrayType(DoubleType, false), false)
  ))

  def dataType: DataType = new TileUDT()

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Array.ofDim[Double](cols * rows).fill(doubleNODATA)
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val col = input.getInt(0)
    val row = input.getInt(1)
    val cell = input.getDouble(2)

    val cells = buffer.getAs[mutable.WrappedArray[Double]](0)

    cells(row * cols + col) = cell

    buffer(0) = cells
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val source = buffer2.getAs[mutable.WrappedArray[Double]](0)
    val dest = buffer1.getAs[mutable.WrappedArray[Double]](0)

    for {
      i ← source.indices
      cell = source(i)
      if isData(cell)
    } dest(i) = cell

    buffer1(0) = dest
  }

  def evaluate(buffer: Row): Any = {
    val cells = buffer.getAs[mutable.WrappedArray[Double]](0).toArray[Double]
    ArrayTile.apply(cells.array, cols, rows).convert(ct)
  }
}
