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
import org.apache.spark.sql.catalyst.util.QuantileSummaries
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.{Column, Row, TypedColumn, types}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.encoders.syntax._
import org.locationtech.rasterframes.encoders.SparkBasicEncoders._
import org.locationtech.rasterframes.expressions.accessors.ExtractTile

case class ApproxCellQuantilesAggregate(probabilities: Seq[Double], relativeError: Double) extends UserDefinedAggregateFunction {
  def inputSchema: StructType = StructType(Seq(
    StructField("value", tileUDT, true)
  ))

  def bufferSchema: StructType = StructType(Seq(
    StructField("buffer", quantileSummariesEncoder.schema, false)
  ))

  def dataType: types.DataType = DataTypes.createArrayType(DataTypes.DoubleType)

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    val qs = new QuantileSummaries(QuantileSummaries.defaultCompressThreshold, relativeError)
    buffer.update(0, qs.toRow)
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val qs = buffer.getStruct(0).as[QuantileSummaries]
    if (!input.isNullAt(0)) {
      val tile = input.getAs[Tile](0)
      var result = qs
      tile.foreachDouble(d => if (!isNoData(d)) result = result.insert(d))

      buffer.update(0, result.toRow)
    }
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val left = buffer1.getStruct(0).as[QuantileSummaries]
    val right = buffer2.getStruct(0).as[QuantileSummaries]
    val merged = left.compress().merge(right.compress())

    val mergedRow = merged.toRow
    buffer1.update(0, mergedRow)
  }

  def evaluate(buffer: Row): Seq[Double] = {
    val summaries = buffer.getStruct(0).as[QuantileSummaries]
    probabilities.flatMap(quantile => summaries.query(quantile))
  }
}

object ApproxCellQuantilesAggregate {
  def apply(
    tile: Column,
    probabilities: Seq[Double],
    relativeError: Double = 0.00001
  ): TypedColumn[Any, Seq[Double]] =
    new ApproxCellQuantilesAggregate(probabilities, relativeError)(ExtractTile(tile))
      .as(s"rf_agg_approx_quantiles")
      .as[Seq[Double]]
}