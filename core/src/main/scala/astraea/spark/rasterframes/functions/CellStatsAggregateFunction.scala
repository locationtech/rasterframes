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

import geotrellis.raster.{Tile, _}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.gt.types.TileUDT
import org.apache.spark.sql.types.{DataType, _}

/**
 * Statistics aggregation function for a full column of tiles.
 *
 * @since 4/17/17
 */
case class CellStatsAggregateFunction() extends UserDefinedAggregateFunction {
  import CellStatsAggregateFunction.C

  override def inputSchema: StructType = StructType(StructField("value", TileUDT) :: Nil)

  override def dataType: DataType =
    StructType(
      Seq(
        StructField("dataCells", LongType),
        StructField("min", DoubleType),
        StructField("max", DoubleType),
        StructField("mean", DoubleType),
        StructField("variance", DoubleType)
      )
    )

  override def bufferSchema: StructType =
    StructType(
      Seq(
        StructField("dataCells", LongType),
        StructField("min", DoubleType),
        StructField("max", DoubleType),
        StructField("sum", DoubleType),
        StructField("sumSqr", DoubleType)
      )
    )

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(C.COUNT) = 0L
    buffer(C.MIN) = Double.MaxValue
    buffer(C.MAX) = Double.MinValue
    buffer(C.SUM) = 0.0
    buffer(C.SUM_SQRS) = 0.0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)) {
      val tile = input.getAs[Tile](0)
      var count = buffer.getLong(C.COUNT)
      var min = buffer.getDouble(C.MIN)
      var max = buffer.getDouble(C.MAX)
      var sum = buffer.getDouble(C.SUM)
      var sumSqr = buffer.getDouble(C.SUM_SQRS)

      tile.foreachDouble(c ⇒ if (isData(c)) {
        count += 1
        min = math.min(min, c)
        max = math.max(max, c)
        sum = sum + c
        sumSqr = sumSqr + c * c
      })

      buffer(C.COUNT) = count
      buffer(C.MIN) = min
      buffer(C.MAX) = max
      buffer(C.SUM) = sum
      buffer(C.SUM_SQRS) = sumSqr
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(C.COUNT) = buffer1.getLong(C.COUNT) + buffer2.getLong(C.COUNT)
    buffer1(C.MIN) = math.min(buffer1.getDouble(C.MIN), buffer2.getDouble(C.MIN))
    buffer1(C.MAX) = math.max(buffer1.getDouble(C.MAX), buffer2.getDouble(C.MAX))
    buffer1(C.SUM) = buffer1.getDouble(C.SUM) + buffer2.getDouble(C.SUM)
    buffer1(C.SUM_SQRS) = buffer1.getDouble(C.SUM_SQRS) + buffer2.getDouble(C.SUM_SQRS)
  }

  override def evaluate(buffer: Row): Any = {
    val count = buffer.getLong(C.COUNT)
    val sum = buffer.getDouble(C.SUM)
    val sumSqr = buffer.getDouble(C.SUM_SQRS)
    val mean = sum / count
    val variance = sumSqr / count - mean * mean
    Row(count, buffer(C.MIN), buffer(C.MAX), mean, variance)
  }
}

object CellStatsAggregateFunction {
  /**  Column index values. */
  private object C {
    val COUNT = 0
    val MIN = 1
    val MAX = 2
    val SUM = 3
    val SUM_SQRS = 4
  }
}
