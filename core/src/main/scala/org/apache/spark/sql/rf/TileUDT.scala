/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea, Inc.
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

package org.apache.spark.sql.rf
import geotrellis.raster._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, _}
import org.locationtech.rasterframes.encoders.CatalystSerializer
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.model.{Cells, TileDataContext}
import org.locationtech.rasterframes.tiles.InternalRowTile


/**
 * UDT for singleband tiles.
 *
 * @since 5/11/17
 */
@SQLUserDefinedType(udt = classOf[TileUDT])
class TileUDT extends UserDefinedType[Tile] {
  import TileUDT._
  override def typeName = TileUDT.typeName

  override def pyUDT: String = "pyrasterframes.rf_types.TileUDT"

  def userClass: Class[Tile] = classOf[Tile]

  def sqlType: StructType = schemaOf[Tile]

  override def serialize(obj: Tile): InternalRow =
    Option(obj)
      .map(_.toInternalRow)
      .orNull

  override def deserialize(datum: Any): Tile =
    Option(datum)
      .collect {
        case ir: InternalRow ⇒ ir.to[Tile]
      }
      .map {
        case realIRT: InternalRowTile ⇒ realIRT.realizedTile
        case other ⇒ other
      }
      .orNull

  override def acceptsType(dataType: DataType): Boolean = dataType match {
    case _: TileUDT ⇒ true
    case _ ⇒ super.acceptsType(dataType)
  }
}

case object TileUDT  {
  UDTRegistration.register(classOf[Tile].getName, classOf[TileUDT].getName)

  final val typeName: String = "tile"

  implicit def tileSerializer: CatalystSerializer[Tile] = new CatalystSerializer[Tile] {

    override val schema: StructType = StructType(Seq(
      StructField("cell_context", schemaOf[TileDataContext], false),
      StructField("cell_data", schemaOf[Cells], false)
    ))

    override def to[R](t: Tile, io: CatalystIO[R]): R = io.create(
      io.to(TileDataContext(t)),
      io.to(Cells(t))
    )

    override def from[R](row: R, io: CatalystIO[R]): Tile = {
      val cells = io.get[Cells](row, 1)

      row match {
        case ir: InternalRow if !cells.isRef ⇒ new InternalRowTile(ir)
        case _ ⇒
          val ctx = io.get[TileDataContext](row, 0)
          cells.toTile(ctx)
      }
    }
  }
}
