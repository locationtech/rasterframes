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
import astraea.spark.rasterframes.encoders.CatalystSerializer.CatalystIO
import geotrellis.raster.Grid
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.types.{ShortType, StructField, StructType}

/**
 * Typed wrapper for tile size information.
 *
 * @since 2018-12-12
 */
case class TileDimensions(cols: Int, rows: Int) extends Grid

object TileDimensions {
  def apply(colsRows: (Int, Int)): TileDimensions = new TileDimensions(colsRows._1, colsRows._2)

  implicit val serializer: CatalystSerializer[TileDimensions] = new CatalystSerializer[TileDimensions] {
    override def schema: StructType = StructType(Seq(
      StructField("cols", ShortType, false),
      StructField("rows", ShortType, false)
    ))

    override protected def to[R](t: TileDimensions, io: CatalystIO[R]): R = io.create(
      t.cols.toShort,
      t.rows.toShort
    )

    override protected def from[R](t: R, io: CatalystIO[R]): TileDimensions = TileDimensions(
      io.getShort(t, 0),
      io.getShort(t, 1)
    )
  }

  implicit val encoder: Encoder[TileDimensions] = CatalystSerializerEncoder[TileDimensions](true)
}