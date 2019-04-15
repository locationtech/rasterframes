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

package astraea.spark.rasterframes.model
import astraea.spark.rasterframes.encoders.{CatalystSerializer, CatalystSerializerEncoder}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.{ShortType, StructField, StructType}
import CatalystSerializer._

case class CellContext(tile_context: TileContext, tile_data_context: TileDataContext, col_index: Short, row_index: Short)
object CellContext {
  implicit val serializer: CatalystSerializer[CellContext] = new CatalystSerializer[CellContext] {
    override def schema: StructType = StructType(Seq(
      StructField("tile_context", schemaOf[TileContext], false),
      StructField("tile_data_context", schemaOf[TileDataContext], false),
      StructField("col_index", ShortType, false),
      StructField("row_index", ShortType, false)
    ))
    override protected def to[R](t: CellContext, io: CatalystSerializer.CatalystIO[R]): R = io.create(
      io.to(t.tile_context),
      io.to(t.tile_data_context),
      t.col_index,
      t.row_index
    )
    override protected def from[R](t: R, io: CatalystSerializer.CatalystIO[R]): CellContext = CellContext(
      io.get[TileContext](t, 0),
      io.get[TileDataContext](t, 1),
      io.getShort(t, 2),
      io.getShort(t, 3)
    )
  }
  implicit def encoder: ExpressionEncoder[CellContext] = CatalystSerializerEncoder[CellContext]()
}
