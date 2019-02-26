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

package astraea.spark.rasterframes.expressions.aggstats

import java.nio.ByteBuffer

import astraea.spark.rasterframes.encoders.CatalystSerializer
import astraea.spark.rasterframes.encoders.CatalystSerializer._
import astraea.spark.rasterframes.functions.safeEval
import astraea.spark.rasterframes.stats.CellHistogram
import geotrellis.raster.Tile
import geotrellis.raster.histogram.{Histogram, StreamingHistogram}
import geotrellis.spark.util.KryoSerializer
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.rf.TileUDT
import org.apache.spark.sql.types._

/**
 * Histogram aggregation function for a full column of tiles.
 *
 * @since 4/24/17
 */
case class HistogramAggregate(numBuckets: Int) extends UserDefinedAggregateFunction {
  def this() = this(StreamingHistogram.DEFAULT_NUM_BUCKETS)

  private val TileType = new TileUDT()

  override def inputSchema: StructType = StructType(StructField("value", TileType) :: Nil)

  override def bufferSchema: StructType = StructType(StructField("buffer", BinaryType) :: Nil)

  override def dataType: DataType = CatalystSerializer[CellHistogram].schema

  override def deterministic: Boolean = true

  @transient
  private lazy val ser = KryoSerializer.ser.newInstance()

  @inline
  private def marshall(hist: Histogram[Double]): Array[Byte] = ser.serialize(hist).array()

  @inline
  private def unmarshall(blob: Array[Byte]): Histogram[Double] = ser.deserialize(ByteBuffer.wrap(blob))

  override def initialize(buffer: MutableAggregationBuffer): Unit =
    buffer(0) = marshall(StreamingHistogram(numBuckets))

  private val safeMerge = safeEval((h1: Histogram[Double], h2: Histogram[Double]) ⇒ h1 merge h2)

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val tile = input.getAs[Tile](0)
    val hist1 = unmarshall(buffer.getAs[Array[Byte]](0))
    val hist2 = safeEval(StreamingHistogram.fromTile(_: Tile, numBuckets))(tile)
    val updatedHist = safeMerge(hist1, hist2)
    buffer(0) = marshall(updatedHist)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val hist1 = unmarshall(buffer1.getAs[Array[Byte]](0))
    val hist2 = unmarshall(buffer2.getAs[Array[Byte]](0))
    val updatedHist = safeMerge(hist1, hist2)
    buffer1(0) = marshall(updatedHist)
  }

  override def evaluate(buffer: Row): Any = {
    val hist = unmarshall(buffer.getAs[Array[Byte]](0))
    CellHistogram(hist).toRow
  }
}

object HistogramAggregate {
  def apply() = new HistogramAggregate(StreamingHistogram.DEFAULT_NUM_BUCKETS)
}
