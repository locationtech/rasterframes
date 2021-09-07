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
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.util.QuantileSummaries
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.{Column, Encoder, Row, TypedColumn, types}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.locationtech.rasterframes.TileType
import org.locationtech.rasterframes.encoders.StandardEncoders
import org.locationtech.rasterframes.expressions.accessors.ExtractTile


case class ApproxCellQuantilesAggregate(probabilities: Seq[Double], relativeError: Double) extends UserDefinedAggregateFunction {
  val quantileSummariesEncoder = StandardEncoders.quantileSummariesEncoder

  override def inputSchema: StructType = StructType(Seq(
    StructField("value", TileType, true)
  ))

  override def bufferSchema: StructType = StructType(Seq(
    StructField("buffer", quantileSummariesEncoder.schema, false)
  ))

  override def dataType: types.DataType = DataTypes.createArrayType(DataTypes.DoubleType)

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    val qs = new QuantileSummaries(QuantileSummaries.defaultCompressThreshold, relativeError)
    val qsRow =
      RowEncoder(quantileSummariesEncoder.schema)
        .resolveAndBind()
        .createDeserializer()(quantileSummariesEncoder.createSerializer()(qs))
    buffer.update(0, qsRow)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val qs = quantileSummariesEncoder
      .resolveAndBind()
      .createDeserializer()(
        RowEncoder(quantileSummariesEncoder.schema)
          .createSerializer()(buffer.getStruct(0))
      )
    if (!input.isNullAt(0)) {
      val tile = input.getAs[Tile](0)
      var result = qs
      tile.foreachDouble(d => if (!isNoData(d)) result = result.insert(d))

      val resultRow =
        RowEncoder(StandardEncoders.quantileSummariesEncoder.schema)
          .resolveAndBind()
          .createDeserializer()(
            StandardEncoders
              .quantileSummariesEncoder
              .createSerializer()(result)
          )

      buffer.update(0, resultRow)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val left = quantileSummariesEncoder
      .resolveAndBind()
      .createDeserializer()(
        RowEncoder(quantileSummariesEncoder.schema)
          .createSerializer()(buffer1.getStruct(0))
      )
    val right = quantileSummariesEncoder
      .resolveAndBind()
      .createDeserializer()(
        RowEncoder(quantileSummariesEncoder.schema)
          .createSerializer()(buffer2.getStruct(0))
      )
    val merged = left.compress().merge(right.compress())

    val mergedRow =
      RowEncoder(StandardEncoders.quantileSummariesEncoder.schema)
        .resolveAndBind()
        .createDeserializer()(
          StandardEncoders
            .quantileSummariesEncoder
            .createSerializer()(merged)
        )

    buffer1.update(0, mergedRow)
  }

  override def evaluate(buffer: Row): Seq[Double] = {
    val summaries = quantileSummariesEncoder
      .resolveAndBind()
      .createDeserializer()(
        RowEncoder(quantileSummariesEncoder.schema)
          .createSerializer()(buffer.getStruct(0))
      )
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
}