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
import astraea.spark.rasterframes.ref.RasterRef
import astraea.spark.rasterframes.ref.RasterRef.RasterRefTile
import geotrellis.raster.{ArrayTile, Tile}
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}

/** Represents the union of binary cell datas or a reference to the data.*/
case class Cells(data: Either[Array[Byte], RasterRef]) {
  def isRef: Boolean = data.isRight
  /** Convert cells into either a RasterRefTile or an ArrayTile. */
  def toTile(ctx: TileDataContext): Tile = {
    data.fold(
      bytes => ArrayTile.fromBytes(bytes, ctx.cellType, ctx.cellColumns, ctx.cellRows),
      ref => RasterRefTile(ref)
    )
  }
}

object Cells {
  /** Extracts the Cells from a Tile. */
  def apply(t: Tile): Cells = {
    t match {
      case ref: RasterRefTile =>
        Cells(Right(ref.rr))
      case o                  =>
        Cells(Left(o.toBytes))
    }
  }

  implicit def tileSerializer: CatalystSerializer[Cells] = new CatalystSerializer[Cells] {
    override def schema: StructType = StructType(Seq(
      StructField("cells", BinaryType, true),
      StructField("ref", CatalystSerializer[RasterRef].schema, true)
    ))
    override protected def to[R](t: Cells, io: CatalystSerializer.CatalystIO[R]): R = io.create(
      t.data.left.getOrElse(null),
      t.data.right.map(rr => io.to(rr)).right.getOrElse(null)
    )
    override protected def from[R](t: R, io: CatalystSerializer.CatalystIO[R]): Cells = {
      if (!io.isNullAt(t, 0))
        Cells(Left(io.getByteArray(t, 0)))
      else if (!io.isNullAt(t, 1))
        Cells(Right(io.get[RasterRef](t, 1)))
      else throw new IllegalArgumentException("must be eithe cell data or a ref, but not null")
    }
  }
}
