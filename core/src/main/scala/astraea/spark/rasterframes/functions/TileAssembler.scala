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

package astraea.spark.rasterframes.functions

import java.nio.{ByteBuffer, DoubleBuffer}

import astraea.spark.rasterframes.util._
import astraea.spark.rasterframes.NOMINAL_TILE_SIZE
import astraea.spark.rasterframes.functions.TileAssembler.TileBuffer
import geotrellis.raster.{DataType => _, _}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  ImperativeAggregate, TypedImperativeAggregate
}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.rf.TileUDT
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, TypedColumn}
import spire.syntax.cfor._

/**
 * Aggregator for reassembling tiles from from exploded form
 *
 * @since 9/24/17
 */
case class TileAssembler(
  colIndex: Expression,
  rowIndex: Expression,
  cellValue: Expression,
  maxTileCols: Short,
  maxTileRows: Short,
  cellType: CellType,
  mutableAggBufferOffset: Int = 0,
  inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[TileBuffer] with ImplicitCastInputTypes {

  override def children: Seq[Expression] = Seq(colIndex, rowIndex, cellValue)

  override def inputTypes = Seq(IntegerType, IntegerType, DoubleType)

  private val TileType = new TileUDT()

  override def prettyName: String = "assemble_tiles"

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def nullable: Boolean = true

  override def dataType: DataType = TileType

  override def createAggregationBuffer(): TileBuffer = new TileBuffer(maxTileCols, maxTileRows)

  @inline
  private def toIndex(col: Int, row: Int) = row * maxTileCols + col

  override def update(buffer: TileBuffer, input: InternalRow): TileBuffer = {
    val col = colIndex.eval(input).asInstanceOf[Int]
    require(col < maxTileCols, s"`maxTileCols` is $maxTileCols, but received index value $col")
    val row = rowIndex.eval(input).asInstanceOf[Int]
    require(row < maxTileRows, s"`maxTileRows` is $maxTileRows, but received index value $row")

    val cell = cellValue.eval(input).asInstanceOf[Double]
    buffer.cellBuffer.put(toIndex(col, row), cell)
    buffer.updateMaxColRow(col, row)
    buffer
  }

  override def merge(buffer: TileBuffer, input: TileBuffer): TileBuffer = {
    val left = buffer.cellBuffer
    val right = input.cellBuffer
    cfor(0)(_ < maxTileRows, _ + 1) { row =>
      cfor(0)(_ < maxTileCols, _ + 1) { col =>
        val cell: Double = right.get(toIndex(col, row))
        if (isData(cell)) {
          left.put(toIndex(col, row), cell)
        }
      }
    }
    val (cMax, rMax) = input.maxColRow
    buffer.updateMaxColRow(cMax, rMax)
    buffer
  }

  override def eval(buffer: TileBuffer): InternalRow = {
    // TODO: figure out how to eliminate copies here.
    val result = buffer.cellBuffer
    val length = result.capacity()
    val cells = Array.ofDim[Double](length)
    result.get(cells)
    val tile = ArrayTile(cells, maxTileCols, maxTileRows).convert(cellType)

    val (cMax, rMax) = buffer.maxColRow

    val cropped = tile.crop(cMax + 1, rMax + 1)
    TileType.serialize(cropped)
  }

  override def serialize(buffer: TileBuffer): Array[Byte] = buffer.serialize()
  override def deserialize(storageFormat: Array[Byte]): TileBuffer = new TileBuffer(storageFormat)
}

object TileAssembler {
  import astraea.spark.rasterframes.encoders.StandardEncoders._

  private val indexPad = 2 * java.lang.Integer.BYTES

  class TileBuffer(val buffer: Array[Byte]) {

    def this(maxTileCols: Int, maxTileRows: Int) =
      this({
        val cellPad = maxTileCols * maxTileRows * java.lang.Double.BYTES
        val tmp = new TileBuffer(Array.ofDim[Byte](cellPad + indexPad))
        tmp.reset()
        tmp.buffer
      })

    def cellBuffer = ByteBuffer.wrap(buffer, 0, buffer.length - indexPad).asDoubleBuffer()
    private def indexBuffer =
      ByteBuffer.wrap(buffer, buffer.length - indexPad, indexPad).asIntBuffer()

    def reset(): Unit = {
      val cells = cellBuffer
      val length = cells.capacity()
      cfor(0)(_ < length, _ + 1) { idx =>
        cells.put(idx, doubleNODATA)
      }
      indexBuffer.put(0, -1).put(1, -1)
    }

    def serialize(): Array[Byte] = buffer

    def maxColRow: (Int, Int) = {
      val indexes = indexBuffer
      (indexes.get(0), indexes.get(1))
    }

    def updateMaxColRow(col: Int, row: Int): Unit = {
      var (cMax, rMax) = maxColRow
      cMax = math.max(cMax, col)
      rMax = math.max(rMax, row)
      indexBuffer.put(0, cMax).put(1, rMax)
    }
  }

  def apply(
    columnIndex: Column,
    rowIndex: Column,
    cellData: Column,
    ct: CellType): TypedColumn[Any, Tile] =
    apply(columnIndex, rowIndex, cellData, NOMINAL_TILE_SIZE, NOMINAL_TILE_SIZE, ct)

  def apply(
    columnIndex: Column,
    rowIndex: Column,
    cellData: Column,
    maxTileCols: Int,
    maxTileRows: Int,
    ct: CellType): TypedColumn[Any, Tile] =
    new Column(new TileAssembler(columnIndex.expr, rowIndex.expr, cellData.expr,
        maxTileCols.toShort, maxTileRows.toShort, ct)
        .toAggregateExpression())
      .as(cellData.columnName)
      .as[Tile]
}
