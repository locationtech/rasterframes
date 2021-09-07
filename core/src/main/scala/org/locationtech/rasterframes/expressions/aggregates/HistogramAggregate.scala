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

package org.locationtech.rasterframes.expressions.aggregates

import java.nio.ByteBuffer

import org.locationtech.rasterframes.expressions.accessors.ExtractTile
import org.locationtech.rasterframes.functions.safeEval
import org.locationtech.rasterframes.stats.CellHistogram
import geotrellis.raster.Tile
import geotrellis.raster.histogram.{Histogram, StreamingHistogram}
import geotrellis.spark.util.KryoSerializer
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, AggregateMode, Complete}
import org.apache.spark.sql.catalyst.expressions.{ExprId, Expression, ExpressionDescription, NamedExpression}
import org.apache.spark.sql.execution.aggregate.ScalaUDAF
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row, TypedColumn}
import org.locationtech.rasterframes.TileType

/**
 * Histogram aggregation function for a full column of tiles.
 *
 * @since 4/24/17
 */
case class HistogramAggregate(numBuckets: Int) extends UserDefinedAggregateFunction {
  def this() = this(StreamingHistogram.DEFAULT_NUM_BUCKETS)
  // TODO: rewrite as TypedAggregateExpression or similar.

  override def inputSchema: StructType = StructType(StructField("value", TileType) :: Nil)

  override def bufferSchema: StructType = StructType(StructField("buffer", BinaryType) :: Nil)

  override def dataType: DataType = CellHistogram.schema

  override def deterministic: Boolean = true

  @transient
  private lazy val ser = KryoSerializer.ser.newInstance()

  @inline
  private def marshall(hist: Histogram[Double]): Array[Byte] = ser.serialize(hist).array()

  @inline
  private def unmarshall(blob: Array[Byte]): Histogram[Double] = ser.deserialize(ByteBuffer.wrap(blob))

  override def initialize(buffer: MutableAggregationBuffer): Unit =
    buffer(0) = marshall(StreamingHistogram(numBuckets))

  private val safeMerge = (h1: Histogram[Double], h2: Histogram[Double]) => (h1, h2) match {
    case (null, null) => null
    case (l, null) => l
    case (null, r) => r
    case (l, r) => l merge r
  }

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

object HistogramAggregate {
  import org.locationtech.rasterframes.encoders.StandardEncoders.cellHistEncoder

  def apply(col: Column): TypedColumn[Any, CellHistogram] =
    apply(col, StreamingHistogram.DEFAULT_NUM_BUCKETS)

  def apply(col: Column, numBuckets: Int): TypedColumn[Any, CellHistogram] =
    new HistogramAggregate(numBuckets)(ExtractTile(col))
      .as(s"rf_agg_approx_histogram($col)")
      .as[CellHistogram]

  /** Adapter hack to allow UserDefinedAggregateFunction to be referenced as an expression. */
  @ExpressionDescription(
    usage = "_FUNC_(tile) - Compute aggregate cell histogram over fa tile column.",
    arguments = """
  Arguments:
    * tile - tile column to analyze""",
    examples = """
  Examples:
    > SELECT _FUNC_(tile);
      ..."""
  )
  class HistogramAggregateUDAF(aggregateFunction: AggregateFunction, mode: AggregateMode, isDistinct: Boolean, filter: Option[Expression], resultId: ExprId)
    extends AggregateExpression(aggregateFunction, mode, isDistinct, filter, resultId) {
    def this(child: Expression) = this(ScalaUDAF(Seq(ExtractTile(child)), new HistogramAggregate()), Complete, false, None, NamedExpression.newExprId)
    override def nodeName: String = "rf_agg_approx_histogram"
  }
  object HistogramAggregateUDAF {
    def apply(child: Expression): HistogramAggregateUDAF = new HistogramAggregateUDAF(child)
  }
}
