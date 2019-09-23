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

package org.locationtech.rasterframes.model

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.{ShortType, StructField, StructType}
import org.locationtech.rasterframes.encoders.{CatalystSerializer, CatalystSerializerEncoder}
import CatalystSerializer._

case class CellContext(tileContext: TileContext, tileDataContext: TileDataContext, colIndex: Short, rowIndex: Short)
object CellContext {
  implicit val serializer: CatalystSerializer[CellContext] = new CatalystSerializer[CellContext] {
    override val schema: StructType = StructType(Seq(
      StructField("tileContext", schemaOf[TileContext], false),
      StructField("tileDataContext", schemaOf[TileDataContext], false),
      StructField("colIndex", ShortType, false),
      StructField("rowIndex", ShortType, false)
    ))
    override protected def to[R](t: CellContext, io: CatalystSerializer.CatalystIO[R]): R = io.create(
      io.to(t.tileContext),
      io.to(t.tileDataContext),
      t.colIndex,
      t.rowIndex
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
