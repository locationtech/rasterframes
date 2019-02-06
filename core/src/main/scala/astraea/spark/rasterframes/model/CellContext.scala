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
import astraea.spark.rasterframes.encoders.CatalystSerializer
import astraea.spark.rasterframes.encoders.CatalystSerializer._
import geotrellis.raster.{CellType, Tile}
import org.apache.spark.sql.types.{ShortType, StringType, StructField, StructType}

case class CellContext(cellType: CellType, cellColumns: Short, cellRows: Short)
object CellContext {
  /** Extracts the CellContext from a Tile. */
  def apply(t: Tile): CellContext = {
    require(t.cols <= Short.MaxValue, s"RasterFrames doesn't support tiles of size ${t.cols}")
    require(t.rows <= Short.MaxValue, s"RasterFrames doesn't support tiles of size ${t.rows}")
    CellContext(
      t.cellType, t.cols.toShort, t.rows.toShort
    )
  }

  implicit def tileSerializer: CatalystSerializer[CellContext] = new CatalystSerializer[CellContext] {
    override def schema: StructType =  StructType(Seq(
      StructField("cell_type", CatalystSerializer[CellType].schema, false),
      StructField("cell_cols", ShortType, false),
      StructField("cell_rows", ShortType, false)
    ))

    override protected def to[R](t: CellContext, io: CatalystIO[R]): R = io.create(
      io.to(t.cellType),
      t.cellColumns,
      t.cellRows
    )
    override protected def from[R](t: R, io: CatalystIO[R]): CellContext = CellContext(
      io.get[CellType](t, 0),
      io.getShort(t, 1),
      io.getShort(t, 2)
    )
  }
}
