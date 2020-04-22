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

package org.locationtech.rasterframes.tiles

import java.nio.ByteBuffer

import geotrellis.raster._
import org.apache.spark.sql.catalyst.InternalRow
import org.locationtech.rasterframes.encoders.CatalystSerializer.CatalystIO
import org.locationtech.rasterframes.model.{Cells, TileDataContext}

/**
 * Wrapper around a `Tile` encoded in a Catalyst `InternalRow`, for the purpose
 * of providing compatible semantics over common operations.
 *
 * @since 11/29/17
 */
class InternalRowTile(val mem: InternalRow) extends DelegatingTile {
  import InternalRowTile._

  override def toArrayTile(): ArrayTile = realizedTile.toArrayTile()

  // TODO: We want to reimplement relevant delegated methods so that they read directly from tungsten storage
  lazy val realizedTile: Tile = cells.toTile(cellContext)

  protected override def delegate: Tile = realizedTile

  private def cellContext: TileDataContext =
    CatalystIO[InternalRow].get[TileDataContext](mem, 0)

  private def cells: Cells = CatalystIO[InternalRow].get[Cells](mem, 1)

  /** Retrieve the cell type from the internal encoding. */
  override def cellType: CellType = cellContext.cellType

  /** Retrieve the number of columns from the internal encoding. */
  override def cols: Int = cellContext.dimensions.cols

  /** Retrieve the number of rows from the internal encoding. */
  override def rows: Int = cellContext.dimensions.rows

  /** Get the internally encoded tile data cells. */
  override lazy val toBytes: Array[Byte] = {
    cells.data.left
      .getOrElse(throw new IllegalStateException(
        "Expected tile cell bytes, but received RasterRef instead: " + cells.data.right.get)
      )
  }

  private lazy val toByteBuffer: ByteBuffer = {
    val data = toBytes
    if(data.length < cols * rows && cellType.name != "bool") {
      // Handling constant tiles like this is inefficient and ugly. All the edge
      // cases associated with them create too much undue complexity for
      // something that's unlikely to be
      // used much in production to warrant handling them specially.
      // If a more efficient handling is necessary, consider a flag in
      // the UDT struct.
      ByteBuffer.wrap(toArrayTile().toBytes())
    } else ByteBuffer.wrap(data)
  }

  /** Reads the cell value at the given index as an Int. */
  def apply(i: Int): Int = cellReader.apply(i)

  /** Reads the cell value at the given index as a Double. */
  def applyDouble(i: Int): Double = cellReader.applyDouble(i)

  def copy = new InternalRowTile(mem.copy)

  private lazy val cellReader: CellReader = {
    cellType match {
      case ct: ByteUserDefinedNoDataCellType ⇒
        ByteUDNDCellReader(this, ct.noDataValue)
      case ct: UByteUserDefinedNoDataCellType ⇒
        UByteUDNDCellReader(this, ct.noDataValue)
      case ct: ShortUserDefinedNoDataCellType ⇒
        ShortUDNDCellReader(this, ct.noDataValue)
      case ct: UShortUserDefinedNoDataCellType ⇒
        UShortUDNDCellReader(this, ct.noDataValue)
      case ct: IntUserDefinedNoDataCellType ⇒
        IntUDNDCellReader(this, ct.noDataValue)
      case ct: FloatUserDefinedNoDataCellType ⇒
        FloatUDNDCellReader(this, ct.noDataValue)
      case ct: DoubleUserDefinedNoDataCellType ⇒
        DoubleUDNDCellReader(this, ct.noDataValue)
      case _: BitCells ⇒ BitCellReader(this)
      case _: ByteCells ⇒ ByteCellReader(this)
      case _: UByteCells ⇒ UByteCellReader(this)
      case _: ShortCells ⇒ ShortCellReader(this)
      case _: UShortCells ⇒ UShortCellReader(this)
      case _: IntCells ⇒ IntCellReader(this)
      case _: FloatCells ⇒ FloatCellReader(this)
      case _: DoubleCells ⇒ DoubleCellReader(this)
    }
  }

  override def toString: String = ShowableTile.show(this)
}

object InternalRowTile {
  sealed trait CellReader {
    def apply(index: Int): Int
    def applyDouble(index: Int): Double
  }

  case class BitCellReader(t: InternalRowTile) extends CellReader {
    def apply(i: Int): Int =
      (t.toByteBuffer.get(i >> 3) >> (i & 7)) & 1 // See BitArrayTile.apply
    def applyDouble(i: Int): Double = apply(i).toDouble
  }

  case class ByteCellReader(t: InternalRowTile) extends CellReader {
    def apply(i: Int): Int = b2i(t.toByteBuffer.get(i))
    def applyDouble(i: Int): Double = b2d(t.toByteBuffer.get(i))
  }

  case class ByteUDNDCellReader(t: InternalRowTile, userDefinedByteNoDataValue: Byte)
    extends CellReader with UserDefinedByteNoDataConversions {
    def apply(i: Int): Int = udb2i(t.toByteBuffer.get(i))
    def applyDouble(i: Int): Double = udb2d(t.toByteBuffer.get(i))
  }

  case class UByteCellReader(t: InternalRowTile) extends CellReader {
    def apply(i: Int): Int = ub2i(t.toByteBuffer.get(i))
    def applyDouble(i: Int): Double = ub2d(t.toByteBuffer.get(i))
  }

  case class UByteUDNDCellReader(t: InternalRowTile, userDefinedByteNoDataValue: Byte)
    extends CellReader with UserDefinedByteNoDataConversions {
    def apply(i: Int): Int = udub2i(t.toByteBuffer.get(i))
    def applyDouble(i: Int): Double = udub2d(t.toByteBuffer.get(i))
  }

  case class ShortCellReader(t: InternalRowTile) extends CellReader {
    def apply(i: Int): Int = s2i(t.toByteBuffer.asShortBuffer().get(i))
    def applyDouble(i: Int): Double = s2d(t.toByteBuffer.asShortBuffer().get(i))
  }

  case class ShortUDNDCellReader(t: InternalRowTile, userDefinedShortNoDataValue: Short)
    extends CellReader with UserDefinedShortNoDataConversions {
    def apply(i: Int): Int = uds2i(t.toByteBuffer.asShortBuffer().get(i))
    def applyDouble(i: Int): Double = uds2d(t.toByteBuffer.asShortBuffer().get(i))
  }

  case class UShortCellReader(t: InternalRowTile) extends CellReader {
    def apply(i: Int): Int = us2i(t.toByteBuffer.asShortBuffer().get(i))
    def applyDouble(i: Int): Double = us2d(t.toByteBuffer.asShortBuffer().get(i))
  }

  case class UShortUDNDCellReader(t: InternalRowTile, userDefinedShortNoDataValue: Short)
    extends CellReader with UserDefinedShortNoDataConversions {
    def apply(i: Int): Int = udus2i(t.toByteBuffer.asShortBuffer().get(i))
    def applyDouble(i: Int): Double = udus2d(t.toByteBuffer.asShortBuffer().get(i))
  }

  case class IntCellReader(t: InternalRowTile) extends CellReader {
    def apply(i: Int): Int = t.toByteBuffer.asIntBuffer().get(i)
    def applyDouble(i: Int): Double = i2d(t.toByteBuffer.asIntBuffer().get(i))
  }

  case class IntUDNDCellReader(t: InternalRowTile, userDefinedIntNoDataValue: Int)
    extends CellReader with UserDefinedIntNoDataConversions {
    def apply(i: Int): Int = udi2i(t.toByteBuffer.asIntBuffer().get(i))
    def applyDouble(i: Int): Double = udi2d(t.toByteBuffer.asIntBuffer().get(i))
  }

  case class FloatCellReader(t: InternalRowTile) extends CellReader {
    def apply(i: Int): Int = f2i(t.toByteBuffer.asFloatBuffer().get(i))
    def applyDouble(i: Int): Double = f2d(t.toByteBuffer.asFloatBuffer().get(i))
  }

  case class FloatUDNDCellReader(t: InternalRowTile, userDefinedFloatNoDataValue: Float)
    extends CellReader with UserDefinedFloatNoDataConversions{
    def apply(i: Int): Int = udf2i(t.toByteBuffer.asFloatBuffer().get(i))
    def applyDouble(i: Int): Double = udf2d(t.toByteBuffer.asFloatBuffer().get(i))
  }

  case class DoubleCellReader(t: InternalRowTile) extends CellReader {
    def apply(i: Int): Int = d2i(t.toByteBuffer.asDoubleBuffer().get(i))
    def applyDouble(i: Int): Double = t.toByteBuffer.asDoubleBuffer().get(i)
  }

  case class DoubleUDNDCellReader(t: InternalRowTile, userDefinedDoubleNoDataValue: Double)
    extends CellReader with UserDefinedDoubleNoDataConversions{
    def apply(i: Int): Int = udd2i(t.toByteBuffer.asDoubleBuffer().get(i))
    def applyDouble(i: Int): Double = udd2d(t.toByteBuffer.asDoubleBuffer().get(i))
  }
}
