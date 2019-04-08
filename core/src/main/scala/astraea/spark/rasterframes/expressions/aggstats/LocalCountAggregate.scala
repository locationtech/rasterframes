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

import astraea.spark.rasterframes.expressions.accessors.ExtractTile
import astraea.spark.rasterframes.functions.safeBinaryOp
import geotrellis.raster.mapalgebra.local.{Add, Defined, Undefined}
import geotrellis.raster.{IntConstantNoDataCellType, Tile}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, AggregateMode, Complete}
import org.apache.spark.sql.catalyst.expressions.{ExprId, Expression, ExpressionDescription, NamedExpression}
import org.apache.spark.sql.execution.aggregate.ScalaUDAF
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{Column, Row, TypedColumn}
import astraea.spark.rasterframes.TileType

/**
 * Catalyst aggregate function that counts `NoData` values in a cell-wise fashion.
 *
 * @param isData true if count should be of non-NoData values, false for NoData values.
 * @since 8/11/17
 */
class LocalCountAggregate(isData: Boolean) extends UserDefinedAggregateFunction {

  private val incCount =
    if (isData) safeBinaryOp((t1: Tile, t2: Tile) ⇒ Add(t1, Defined(t2)))
    else safeBinaryOp((t1: Tile, t2: Tile) ⇒ Add(t1, Undefined(t2)))

  private val add = safeBinaryOp(Add.apply(_: Tile, _: Tile))

  override def dataType: DataType = TileType

  override def inputSchema: StructType = StructType(Seq(
    StructField("value", TileType, true)
  ))

  override def bufferSchema: StructType = inputSchema

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit =
    buffer(0) = null

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val right = input.getAs[Tile](0)
    if (right != null) {
      if (buffer(0) == null) {
        buffer(0) = (
          if (isData) Defined(right) else Undefined(right)
          ).convert(IntConstantNoDataCellType)
      } else {
        val left = buffer.getAs[Tile](0)
        buffer(0) = incCount(left, right)
      }
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = add(buffer1.getAs[Tile](0), buffer2.getAs[Tile](0))
  }

  override def evaluate(buffer: Row): Tile = buffer.getAs[Tile](0)
}
object LocalCountAggregate {
  import astraea.spark.rasterframes.encoders.StandardEncoders.singlebandTileEncoder
  @ExpressionDescription(
    usage = "_FUNC_(tile) - Compute cell-wise count of non-no-data values."
  )
  class LocalDataCellsUDAF(aggregateFunction: AggregateFunction, mode: AggregateMode, isDistinct: Boolean, resultId: ExprId) extends AggregateExpression(aggregateFunction, mode, isDistinct, resultId) {
    def this(child: Expression) = this(ScalaUDAF(Seq(ExtractTile(child)), new LocalCountAggregate(true)), Complete, false, NamedExpression.newExprId)
    override def nodeName: String = "agg_local_data_cells"
  }
  object LocalDataCellsUDAF {
    def apply(child: Expression): LocalDataCellsUDAF = new LocalDataCellsUDAF(child)
    def apply(tile: Column): TypedColumn[Any, Tile] =
      new Column(new LocalDataCellsUDAF(tile.expr))
        .as(s"agg_local_data_cells($tile)")
        .as[Tile]
  }

  @ExpressionDescription(
    usage = "_FUNC_(tile) - Compute cell-wise count of no-data values."
  )
  class LocalNoDataCellsUDAF(aggregateFunction: AggregateFunction, mode: AggregateMode, isDistinct: Boolean, resultId: ExprId) extends AggregateExpression(aggregateFunction, mode, isDistinct, resultId) {
    def this(child: Expression) = this(ScalaUDAF(Seq(ExtractTile(child)), new LocalCountAggregate(false)), Complete, false, NamedExpression.newExprId)
    override def nodeName: String = "agg_local_no_data_cells"
  }
  object LocalNoDataCellsUDAF {
    def apply(child: Expression): LocalNoDataCellsUDAF = new LocalNoDataCellsUDAF(child)
    def apply(tile: Column): TypedColumn[Any, Tile] =
      new Column(new LocalNoDataCellsUDAF(tile.expr))
        .as(s"agg_local_no_data_cells($tile)")
        .as[Tile]
  }

}
