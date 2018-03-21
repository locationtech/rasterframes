/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Astraea, Inc.
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
 */

package org.apache.spark.sql.gt.types


import geotrellis.raster._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, _}
import org.apache.spark.unsafe.types.UTF8String

/**
 * UDT for singleband tiles.
 *
 * @author sfitch
 * @since 5/11/17
 */
class TileUDT extends UserDefinedType[Tile] {
  import TileUDT._
  override def typeName = "rf_tile"

  def sqlType = StructType(Seq(
    StructField("cellType", StringType, false),
    StructField("cols", ShortType, false),
    StructField("rows", ShortType, false),
    StructField("data", BinaryType, false)
  ))

  override def serialize(obj: Tile): Any = {
    Option(obj)
      .map(tile ⇒ {
        InternalRow(
          UTF8String.fromString(tile.cellType.name),
          tile.cols.toShort,
          tile.rows.toShort,
          tile.toBytes)
      })
      .orNull
  }

  override def deserialize(datum: Any): Tile = {
    Option(datum)
      .collect { case row: InternalRow ⇒
        val ctName = row.getString(C.CELL_TYPE)
        val cols = row.getShort(C.COLS)
        val rows = row.getShort(C.ROWS)
        val data = row.getBinary(C.DATA)
        val cellType = CellType.fromName(ctName)
        // This is likely a time bomb, but short of adding a flag, not sure how else to do this.
        if(data.length < cols * rows && !cellType.isInstanceOf[BitCells])
          constantTileFromBytes(data, cellType, cols, rows)
        else
          ArrayTile.fromBytes(data, cellType, cols, rows)
      }
      .orNull
  }

  def userClass: Class[Tile] = classOf[Tile]

  private[sql] override def acceptsType(dataType: DataType) = dataType match {
    case _: TileUDT ⇒ true
    case _ ⇒ super.acceptsType(dataType)
  }
}

case object TileUDT extends TileUDT {
  UDTRegistration.register(classOf[Tile].getName, classOf[TileUDT].getName)

  // Temporary, until GeoTrellis 1.2
  // See https://github.com/locationtech/geotrellis/pull/2401
  private[TileUDT] def constantTileFromBytes(bytes: Array[Byte], t: CellType, cols: Int, rows: Int): ConstantTile =
    t match {
      case _: BitCells =>
        BitConstantTile(BitArrayTile.fromBytes(bytes, 1, 1).array(0), cols, rows)
      case ct: ByteCells =>
        ByteConstantTile(ByteArrayTile.fromBytes(bytes, 1, 1, ct).array(0), cols, rows, ct)
      case ct: UByteCells =>
        UByteConstantTile(UByteArrayTile.fromBytes(bytes, 1, 1, ct).array(0), cols, rows, ct)
      case ct: ShortCells =>
        ShortConstantTile(ShortArrayTile.fromBytes(bytes, 1, 1, ct).array(0), cols, rows, ct)
      case ct: UShortCells =>
        UShortConstantTile(UShortArrayTile.fromBytes(bytes, 1, 1, ct).array(0), cols, rows, ct)
      case ct: IntCells =>
        IntConstantTile(IntArrayTile.fromBytes(bytes, 1, 1, ct).array(0), cols, rows, ct)
      case ct: FloatCells =>
        FloatConstantTile(FloatArrayTile.fromBytes(bytes, 1, 1, ct).array(0), cols, rows, ct)
      case ct: DoubleCells =>
        DoubleConstantTile(DoubleArrayTile.fromBytes(bytes, 1, 1, ct).array(0), cols, rows, ct)
    }

  object C {
    val CELL_TYPE = 0
    val COLS = 1
    val ROWS = 2
    val DATA = 3
  }
}
