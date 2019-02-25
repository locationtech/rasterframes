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

import astraea.spark.rasterframes.functions.safeBinaryOp
import geotrellis.raster.Tile
import geotrellis.raster.mapalgebra.local.LocalTileBinaryOp
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.rf.TileUDT
import org.apache.spark.sql.types._

/**
 * Aggregation function for applying a [[LocalTileBinaryOp]] pairwise across all tiles. Assumes Monoid algebra.
 *
 * @since 4/17/17
 */
class LocalTileOpAggregate(op: LocalTileBinaryOp) extends UserDefinedAggregateFunction {

  private val safeOp = safeBinaryOp(op.apply(_: Tile, _: Tile))

  private val TileType = new TileUDT()

  override def inputSchema: StructType = StructType(Seq(
    StructField("value", TileType, true)
  ))

  override def bufferSchema: StructType = inputSchema

  override def dataType: DataType = TileType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit =
    buffer(0) = null

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (buffer(0) == null) {
      buffer(0) = input(0)
    } else {
      val t1 = buffer.getAs[Tile](0)
      val t2 = input.getAs[Tile](0)
      buffer(0) = safeOp(t1, t2)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = update(buffer1, buffer2)

  override def evaluate(buffer: Row): Tile = buffer.getAs[Tile](0)
}
