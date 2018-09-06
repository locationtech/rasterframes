/*
 * Copyright 2017 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package astraea.spark.rasterframes.functions

import geotrellis.raster.mapalgebra.local._
import geotrellis.raster.{DoubleConstantNoDataCellType, IntConstantNoDataCellType, IntUserDefinedNoDataCellType, Tile, isNoData}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import DataBiasedOp._
import org.apache.spark.sql.rf.TileUDT


/**
 * Aggregation function for computing multiple local (cell-wise) statistics across all tiles.
 *
 * @since 4/17/17
 */
class LocalStatsAggregateFunction() extends UserDefinedAggregateFunction {
  import LocalStatsAggregateFunction.C
  private val reafiableUDT = new TileUDT()

  override def inputSchema: StructType = StructType(StructField("value", TileUDT) :: Nil)

  override def dataType: DataType =
    StructType(
      Seq(
        StructField("count", reafiableUDT),
        StructField("min", reafiableUDT),
        StructField("max", reafiableUDT),
        StructField("mean", reafiableUDT),
        StructField("variance", reafiableUDT)
      )
    )

  override def bufferSchema: StructType =
    StructType(
      Seq(
        StructField("count", TileUDT),
        StructField("min", TileUDT),
        StructField("max", TileUDT),
        StructField("sum", TileUDT),
        StructField("sumSqr", TileUDT)
      )
    )

  private val initFunctions = Seq(
    (t: Tile) ⇒ Defined(t).convert(IntConstantNoDataCellType),
    (t: Tile) ⇒ t,
    (t: Tile) ⇒ t,
    (t: Tile) ⇒ t.convert(DoubleConstantNoDataCellType),
    (t: Tile) ⇒ { val d = t.convert(DoubleConstantNoDataCellType); Multiply(d, d) }
  )

  private val updateFunctions = Seq(
    safeBinaryOp((agg: Tile, t: Tile) ⇒ BiasedAdd(agg, Defined(t))),
    safeBinaryOp((agg: Tile, t: Tile) ⇒ BiasedMin(agg, t)),
    safeBinaryOp((agg: Tile, t: Tile) ⇒ BiasedMax(agg, t)),
    safeBinaryOp((agg: Tile, t: Tile) ⇒ BiasedAdd(agg, t)),
    safeBinaryOp((agg: Tile, t: Tile) ⇒ {
      val d = t.convert(DoubleConstantNoDataCellType)
      BiasedAdd(agg, Multiply(d, d))
    })
  )

  private val mergeFunctions = Seq(
    safeBinaryOp((t1: Tile, t2: Tile) ⇒ BiasedAdd(t1, t2)),
    updateFunctions(C.MIN),
    updateFunctions(C.MAX),
    updateFunctions(C.SUM),
    safeBinaryOp((t1: Tile, t2: Tile) ⇒ BiasedAdd(t1, t2))
  )

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    for(i ← initFunctions.indices) {
      buffer(i) = null
    }
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val right = input.getAs[Tile](0)
    if (right != null) {
      for (i ← initFunctions.indices) {
        if (buffer.isNullAt(i)) {
          buffer(i) = initFunctions(i)(right)
        }
        else {
          val left = buffer.getAs[Tile](i)
          buffer(i) = updateFunctions(i)(left, right)
        }
      }
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    for (i ← mergeFunctions.indices) {
      val left = buffer1.getAs[Tile](i)
      val right = buffer2.getAs[Tile](i)
      val merged = mergeFunctions(i)(left, right)
      buffer1(i) = merged
    }
  }

  override def evaluate(buffer: Row): Any = {
    val cnt = buffer.getAs[Tile](C.COUNT)
    if (cnt != null) {
      val count = cnt.interpretAs(IntUserDefinedNoDataCellType(0))
      val sum = buffer.getAs[Tile](C.SUM)
      val sumSqr = buffer.getAs[Tile](C.SUM_SQRS)
      val mean = sum / count
      val meanSqr = mean * mean
      val variance = (sumSqr / count) - meanSqr
      Row(count, buffer(C.MIN), buffer(C.MAX), mean, variance)
    } else null
  }
}

object LocalStatsAggregateFunction {
  /**  Column index values. */
  private object C {
    val COUNT = 0
    val MIN = 1
    val MAX = 2
    val SUM = 3
    val SUM_SQRS = 4
  }
}

