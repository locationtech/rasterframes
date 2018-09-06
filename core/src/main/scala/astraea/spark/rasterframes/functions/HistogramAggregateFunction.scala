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

import java.nio.ByteBuffer

import astraea.spark.rasterframes.encoders.StandardEncoders
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
case class HistogramAggregateFunction(numBuckets: Int) extends UserDefinedAggregateFunction {
  def this() = this(StreamingHistogram.DEFAULT_NUM_BUCKETS)

  override def inputSchema: StructType = StructType(StructField("value", TileUDT) :: Nil)

  override def bufferSchema: StructType = StructType(StructField("buffer", BinaryType) :: Nil)

  override def dataType: DataType = StandardEncoders.histEncoder.schema

  override def deterministic: Boolean = true


  @inline
  private def marshall(hist: Histogram[Double]): Array[Byte] =
    HistogramAggregateFunction.ser.serialize(hist).array()

  @inline
  private def unmarshall(blob: Array[Byte]): Histogram[Double] =
    HistogramAggregateFunction.ser.deserialize(ByteBuffer.wrap(blob))

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
    CellHistogram(hist)
  }
}

object HistogramAggregateFunction {
  def apply() = new HistogramAggregateFunction(StreamingHistogram.DEFAULT_NUM_BUCKETS)
  @transient
  private lazy val ser = KryoSerializer.ser.newInstance()
}
