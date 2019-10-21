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

import geotrellis.raster.Dimensions
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.locationtech.rasterframes.encoders.CatalystSerializer
import org.locationtech.rasterframes.encoders.CatalystSerializer.CatalystIO

/**
 * Typed wrapper for tile size information.
 *
 * @since 2018-12-12
 */
case class TileDimensions(cols: Int, rows: Int)

object TileDimensions {
  def apply(colsRows: (Int, Int)): TileDimensions = new TileDimensions(colsRows._1, colsRows._2)
  def apply(dims: Dimensions[Int]): TileDimensions = new TileDimensions(dims.cols, dims.rows)

  implicit val serializer: CatalystSerializer[TileDimensions] = new CatalystSerializer[TileDimensions] {
    override val schema: StructType = StructType(Seq(
      StructField("cols", IntegerType, false),
      StructField("rows", IntegerType, false)
    ))

    override protected def to[R](t: TileDimensions, io: CatalystIO[R]): R = io.create(
      t.cols,
      t.rows
    )

    override protected def from[R](t: R, io: CatalystIO[R]): TileDimensions = TileDimensions(
      io.getInt(t, 0),
      io.getInt(t, 1)
    )
  }

  implicit def encoder: ExpressionEncoder[TileDimensions] = ExpressionEncoder[TileDimensions]()
}
