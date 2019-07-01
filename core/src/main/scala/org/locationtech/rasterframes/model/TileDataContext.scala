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

import org.locationtech.rasterframes.encoders.CatalystSerializer._
import geotrellis.raster.{CellType, Tile}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.{StructField, StructType}
import org.locationtech.rasterframes.encoders.{CatalystSerializer, CatalystSerializerEncoder}

/** Encapsulates all information about a tile aside from actual cell values. */
case class TileDataContext(cellType: CellType, dimensions: TileDimensions)
object TileDataContext {

  /** Extracts the TileDataContext from a Tile. */
  def apply(t: Tile): TileDataContext = {
    require(t.cols <= Short.MaxValue, s"RasterFrames doesn't support tiles of size ${t.cols}")
    require(t.rows <= Short.MaxValue, s"RasterFrames doesn't support tiles of size ${t.rows}")
    TileDataContext(
      t.cellType, TileDimensions(t.dimensions)
    )
  }

  implicit val serializer: CatalystSerializer[TileDataContext] = new CatalystSerializer[TileDataContext] {
    override def schema: StructType =  StructType(Seq(
      StructField("cellType", schemaOf[CellType], false),
      StructField("dimensions", schemaOf[TileDimensions], false)
    ))

    override protected def to[R](t: TileDataContext, io: CatalystIO[R]): R = io.create(
      io.to(t.cellType),
      io.to(t.dimensions)
    )
    override protected def from[R](t: R, io: CatalystIO[R]): TileDataContext = TileDataContext(
      io.get[CellType](t, 0),
      io.get[TileDimensions](t, 1)
    )
  }

  implicit def encoder: ExpressionEncoder[TileDataContext] = CatalystSerializerEncoder[TileDataContext]()
}
