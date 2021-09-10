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

import org.locationtech.rasterframes._
import org.locationtech.rasterframes.expressions.accessors.ExtractTile
import org.locationtech.rasterframes.functions.safeBinaryOp
import org.locationtech.rasterframes.util.DataBiasedOp.{BiasedMax, BiasedMin}
import geotrellis.raster.Tile
import geotrellis.raster.mapalgebra.local.LocalTileBinaryOp
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, AggregateMode, Complete}
import org.apache.spark.sql.catalyst.expressions.{ExprId, Expression, ExpressionDescription, NamedExpression}
import org.apache.spark.sql.execution.aggregate.ScalaUDAF
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row, TypedColumn}

/**
 * Aggregation function for applying a [[LocalTileBinaryOp]] pairwise across all tiles. Assumes Monoid algebra.
 *
 * @since 4/17/17
 */
class LocalTileOpAggregate(op: LocalTileBinaryOp) extends UserDefinedAggregateFunction {

  private val safeOp = safeBinaryOp(op.apply(_: Tile, _: Tile))

  override def inputSchema: StructType = StructType(Seq(
    StructField("value", dataType, true)
  ))

  override def bufferSchema: StructType = inputSchema

  override def dataType: DataType = tileUDT

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) = null

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit =
    if (buffer(0) == null) {
      buffer(0) = input(0)
    } else {
      val t1 = buffer.getAs[Tile](0)
      val t2 = input.getAs[Tile](0)
      buffer(0) = safeOp(t1, t2)
    }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = update(buffer1, buffer2)

  override def evaluate(buffer: Row): Tile = buffer.getAs[Tile](0)
}

object LocalTileOpAggregate {
  @ExpressionDescription(
    usage = "_FUNC_(tile) - Compute cell-wise minimum value from a tile column."
  )
  class LocalMinUDAF(aggregateFunction: AggregateFunction, mode: AggregateMode, isDistinct: Boolean, filter: Option[Expression], resultId: ExprId) extends AggregateExpression(aggregateFunction, mode, isDistinct, filter, resultId) {
    def this(child: Expression) = this(ScalaUDAF(Seq(ExtractTile(child)), new LocalTileOpAggregate(BiasedMin)), Complete, false, None, NamedExpression.newExprId)
    override def nodeName: String = "rf_agg_local_min"
  }
  object LocalMinUDAF {
    def apply(child: Expression): LocalMinUDAF = new LocalMinUDAF(child)
    def apply(tile: Column): TypedColumn[Any, Tile] =
      new LocalTileOpAggregate(BiasedMin)(ExtractTile(tile))
        .as(s"rf_agg_local_min($tile)")
        .as[Tile]
  }

  @ExpressionDescription(
    usage = "_FUNC_(tile) - Compute cell-wise maximum value from a tile column."
  )
  class LocalMaxUDAF(aggregateFunction: AggregateFunction, mode: AggregateMode, isDistinct: Boolean, filter: Option[Expression], resultId: ExprId) extends AggregateExpression(aggregateFunction, mode, isDistinct, filter, resultId) {
    def this(child: Expression) = this(ScalaUDAF(Seq(ExtractTile(child)), new LocalTileOpAggregate(BiasedMax)), Complete, false, None, NamedExpression.newExprId)
    override def nodeName: String = "rf_agg_local_max"
  }
  object LocalMaxUDAF {
    def apply(child: Expression): LocalMaxUDAF = new LocalMaxUDAF(child)
    def apply(tile: Column): TypedColumn[Any, Tile] =
      new LocalTileOpAggregate(BiasedMax)(ExtractTile(tile))
        .as(s"rf_agg_local_max($tile)")
        .as[Tile]
  }
}
