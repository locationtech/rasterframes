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

import geotrellis.raster.{ArrayTile, BufferTile, CellType, ConstantTile, GridBounds, Tile}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.encoders.syntax._
import org.locationtech.rasterframes.ref.RasterRef
import org.locationtech.rasterframes.tiles.{ProjectedRasterTile, ShowableTile}

import scala.util.Try

/**
 * UDT for singleband tiles.
 *
 * @since 5/11/17
 */
@SQLUserDefinedType(udt = classOf[TileUDT])
class TileUDT extends UserDefinedType[Tile] {
  override def typeName = TileUDT.typeName

  override def pyUDT: String = "pyrasterframes.rf_types.TileUDT"

  def userClass: Class[Tile] = classOf[Tile]

  def sqlType: StructType = StructType(Seq(
    StructField("cellType", StringType, false),
    StructField("cols", IntegerType, false),
    StructField("rows", IntegerType, false),
    StructField("cells", BinaryType, true),
    StructField("gridBounds", gridBoundsEncoder[Int].schema, true),
    // make it parquet compliant, only expanded UDTs can be in a UDT schema
    StructField("ref", ParquetReadSupport.expandUDT(RasterRef.rasterRefEncoder.schema), true)
  ))

  def serialize(obj: Tile): InternalRow = {
    if (obj == null) return null
    obj match {
      // TODO: review matches there
      case ref: RasterRef =>
        val ct = UTF8String.fromString(ref.cellType.toString())
        InternalRow(ct, ref.cols, ref.rows, null, null, ref.toInternalRow)
      case ProjectedRasterTile(ref: RasterRef, _, _) =>
        val ct = UTF8String.fromString(ref.cellType.toString())
        InternalRow(ct, ref.cols, ref.rows, null, null, ref.toInternalRow)
      case prt: ProjectedRasterTile =>
        val tile = prt.tile
        val ct = UTF8String.fromString(tile.cellType.toString())
        InternalRow(ct, tile.cols, tile.rows, tile.toBytes(), null, null)
      case bt: BufferTile =>
        val tile = bt.sourceTile.toArrayTile()
        val ct = UTF8String.fromString(tile.cellType.toString())
        InternalRow(ct, tile.cols, tile.rows, tile.toBytes(), bt.gridBounds.toInternalRow, null)
      case const: ConstantTile =>
        // Must expand constant tiles so they can be interpreted properly in catalyst and Python.
        val tile = const.toArrayTile()
        val ct = UTF8String.fromString(tile.cellType.toString())
        InternalRow(ct, tile.cols, tile.rows, tile.toBytes(), null, null)
      case tile =>
        val ct = UTF8String.fromString(tile.cellType.toString())
        InternalRow(ct, tile.cols, tile.rows, tile.toBytes(), null, null)
    }
  }

  def deserialize(datum: Any): Tile = {
    if (datum == null) return null
    val row = datum.asInstanceOf[InternalRow]

    /** TODO: a compatible encoder for the ProjectedRasterTile? */
    val tile: Tile =
      if (!row.isNullAt(5)) {
        Try {
          val ir = row.getStruct(5, 5)
          val ref = ir.as[RasterRef]
          ref
        }/*.orElse {
          Try(
            ProjectedRasterTile
              .projectedRasterTileEncoder
              .resolveAndBind()
              .createDeserializer()(row)
              .tile
          )
        }*/.get
      } else if(!row.isNullAt(4)) {
        val ct = CellType.fromName(row.getString(0))
        val cols = row.getInt(1)
        val rows = row.getInt(2)
        val bytes = row.getBinary(3)
        val gridBounds = row.getStruct(4, 5).as[GridBounds[Int]]
        BufferTile(ArrayTile.fromBytes(bytes, ct, cols, rows), gridBounds)
      } else {
        val ct = CellType.fromName(row.getString(0))
        val cols = row.getInt(1)
        val rows = row.getInt(2)
        val bytes = row.getBinary(3)
        ArrayTile.fromBytes(bytes, ct, cols, rows)
      }

    if (TileUDT.showableTiles) new ShowableTile(tile) else tile
  }

  override def acceptsType(dataType: DataType): Boolean = dataType match {
    case _: TileUDT => true
    case _ => super.acceptsType(dataType)
  }
}

case object TileUDT {
  private val showableTiles = org.locationtech.rasterframes.rfConfig.getBoolean("showable-tiles")

  UDTRegistration.register(classOf[Tile].getName, classOf[TileUDT].getName)

  final val typeName: String = "tile"
}
