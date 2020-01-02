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

import geotrellis.raster.{Tile, isNoData}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, AggregateMode, Complete}
import org.apache.spark.sql.catalyst.expressions.{ExprId, Expression, ExpressionDescription, NamedExpression}
import org.apache.spark.sql.catalyst.util.QuantileSummaries
import org.apache.spark.sql.execution.aggregate.ScalaUDAF
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.{Column, Encoder, Row, TypedColumn, types}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.locationtech.rasterframes.TileType
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.expressions.accessors.ExtractTile


case class ApproxCellQuantilesAggregate(probabilities: Seq[Double], relativeError: Double) extends UserDefinedAggregateFunction {
  import org.locationtech.rasterframes.encoders.StandardSerializers.quantileSerializer

  override def inputSchema: StructType = StructType(Seq(
    StructField("value", TileType, true)
  ))

  override def bufferSchema: StructType = StructType(Seq(
    StructField("buffer", schemaOf[QuantileSummaries], false)
  ))

  override def dataType: types.DataType = DataTypes.createArrayType(DataTypes.DoubleType)

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit =
    buffer.update(0, new QuantileSummaries(QuantileSummaries.defaultCompressThreshold, relativeError).toRow)

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val qs = buffer.getStruct(0).to[QuantileSummaries]
    if (!input.isNullAt(0)) {
      val tile = input.getAs[Tile](0)
      var result = qs
      tile.foreachDouble(d => if (!isNoData(d)) result = result.insert(d))
      buffer.update(0, result.toRow)
    }
    else buffer
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val left = buffer1.getStruct(0).to[QuantileSummaries]
    val right = buffer2.getStruct(0).to[QuantileSummaries]
    val merged = left.compress().merge(right.compress())
    buffer1.update(0, merged.toRow)
  }

  override def evaluate(buffer: Row): Seq[Double] = {
    val summaries = buffer.getStruct(0).to[QuantileSummaries]
    probabilities.flatMap(summaries.query)
  }
}

object ApproxCellQuantilesAggregate {
  private implicit def doubleSeqEncoder: Encoder[Seq[Double]] = ExpressionEncoder()

  def apply(
    tile: Column,
    probabilities: Seq[Double],
    relativeError: Double = 0.00001): TypedColumn[Any, Seq[Double]] = {
    new ApproxCellQuantilesAggregate(probabilities, relativeError)(ExtractTile(tile))
      .as(s"rf_agg_approx_quantiles")
      .as[Seq[Double]]
  }

  /** Adapter hack to allow UserDefinedAggregateFunction to be referenced as an expression. */
  @ExpressionDescription(
    usage = "_FUNC_(tile, probabilities, relativeError) - Compute aggregate cell histogram over a tile column.",
    arguments = """
  Arguments:
    * tile - tile column to analyze
    * probabilities - array of double values in [0, 1] at which to compute quantiles
    * relativeError - non-negative error tolerance""",
    examples = """
  Examples:
    > SELECT _FUNC_(tile, array(0.1, 0.25, 0.5, 0.75, 0.9), 0.001);
      ..."""
  )
  class ApproxCellQuantilesUDAF(aggregateFunction: AggregateFunction, mode: AggregateMode, isDistinct: Boolean, resultId: ExprId)
    extends AggregateExpression(aggregateFunction, mode, isDistinct, resultId) {
    def this(child: Expression, probabilities: Seq[Double], relativeError: Double) =
      this(ScalaUDAF(Seq(ExtractTile(child)), new ApproxCellQuantilesAggregate(probabilities, relativeError)), Complete, false, NamedExpression.newExprId)
    override def nodeName: String = "rf_agg_approx_quantiles"
  }

  object ApproxCellQuantilesUDAF {
    def apply(child: Expression, probabilities: Seq[Double], relativeError: Double): ApproxCellQuantilesUDAF = new ApproxCellQuantilesUDAF(child, probabilities, relativeError)
    def apply(child: Expression, probabilities: Seq[Double]): ApproxCellQuantilesUDAF = new ApproxCellQuantilesUDAF(child, probabilities, 0.00001)
  }
}