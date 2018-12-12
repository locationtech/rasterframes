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

package astraea.spark.rasterframes.encoders

import astraea.spark.rasterframes.encoders.CatalystSerializer.CatalystIO
import astraea.spark.rasterframes.ref.{RasterRef, RasterSource}
import com.vividsolutions.jts.geom.Envelope
import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, Tile}
import geotrellis.vector.Extent
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.rf.{RasterSourceUDT, TileUDT}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Typeclass for converting to/from JVM object to catalyst encoding. The reason this exists is that
 * instantiating and binding `ExpressionEncoder[T]` is *very* expensive, and not suitable for
 * operations internal to an  `Expression`.
 *
 * @since 10/19/18
 */
trait CatalystSerializer[T] extends Serializable {
  def schema: StructType
  protected def to[R](t: T, io: CatalystIO[R]): R
  protected def from[R](t: R, io: CatalystIO[R]): T

  final def toRow(t: T): Row = to(t, CatalystIO[Row])
  final def fromRow(row: Row): T = from(row, CatalystIO[Row])

  final def toInternalRow(t: T): InternalRow = to(t, CatalystIO[InternalRow])
  final def fromInternalRow(row: InternalRow): T = from(row, CatalystIO[InternalRow])
}

object CatalystSerializer {
  def apply[T: CatalystSerializer]: CatalystSerializer[T] = implicitly

  /**
   * For some reason `Row` and `InternalRow` share no common base type. Instead of using
   * structural types (which use reflection), this typeclass is used to normalize access
   * to the underlying storage construct.
   *
   * @tparam R row storage type
   */
  trait CatalystIO[R] extends Serializable {
    def create(values: Any*): R
    def to[T: CatalystSerializer](t: T): R = CatalystSerializer[T].to(t, this)
    def isNullAt(d: R, ordinal: Int): Boolean
    def getBoolean(d: R, ordinal: Int): Boolean
    def getByte(d: R, ordinal: Int): Byte
    def getShort(d: R, ordinal: Int): Short
    def getInt(d: R, ordinal: Int): Int
    def getLong(d: R, ordinal: Int): Long
    def getFloat(d: R, ordinal: Int): Float
    def getDouble(d: R, ordinal: Int): Double
    def getString(d: R, ordinal: Int): String
    def getByteArray(d: R, ordinal: Int): Array[Byte]
    def get[T: CatalystSerializer](d: R, ordinal: Int): T
    def encode(str: String): AnyRef
  }

  object CatalystIO {
    def apply[R: CatalystIO]: CatalystIO[R] = implicitly

    trait AbstractRowEncoder[R <: Row] extends CatalystIO[R] {
      override def isNullAt(d: R, ordinal: Int): Boolean = d.isNullAt(ordinal)
      override def getBoolean(d: R, ordinal: Int): Boolean = d.getBoolean(ordinal)
      override def getByte(d: R, ordinal: Int): Byte = d.getByte(ordinal)
      override def getShort(d: R, ordinal: Int): Short = d.getShort(ordinal)
      override def getInt(d: R, ordinal: Int): Int = d.getInt(ordinal)
      override def getLong(d: R, ordinal: Int): Long = d.getLong(ordinal)
      override def getFloat(d: R, ordinal: Int): Float =  d.getFloat(ordinal)
      override def getDouble(d: R, ordinal: Int): Double = d.getDouble(ordinal)
      override def getString(d: R, ordinal: Int): String = d.getString(ordinal)
      override def getByteArray(d: R, ordinal: Int): Array[Byte] = d.get(ordinal).asInstanceOf[Array[Byte]]
      override def get[T: CatalystSerializer](d: R, ordinal: Int): T = {
        val struct = d.getStruct(ordinal)
        struct.to[T]
      }
      override def encode(str: String): String = str
    }

    implicit val rowIO: CatalystIO[Row] = new AbstractRowEncoder[Row] {
      override def create(values: Any*): Row = Row(values: _*)
    }

    implicit val internalRowIO: CatalystIO[InternalRow] = new CatalystIO[InternalRow] {
      override def isNullAt(d: InternalRow, ordinal: Int): Boolean = d.isNullAt(ordinal)
      override def getBoolean(d: InternalRow, ordinal: Int): Boolean = d.getBoolean(ordinal)
      override def getByte(d: InternalRow, ordinal: Int): Byte = d.getByte(ordinal)
      override def getShort(d: InternalRow, ordinal: Int): Short = d.getShort(ordinal)
      override def getInt(d: InternalRow, ordinal: Int): Int = d.getInt(ordinal)
      override def getLong(d: InternalRow, ordinal: Int): Long = d.getLong(ordinal)
      override def getFloat(d: InternalRow, ordinal: Int): Float = d.getFloat(ordinal)
      override def getDouble(d: InternalRow, ordinal: Int): Double = d.getDouble(ordinal)
      override def getString(d: InternalRow, ordinal: Int): String = d.getString(ordinal)
      override def getByteArray(d: InternalRow, ordinal: Int): Array[Byte] = d.getBinary(ordinal)
      override def get[T: CatalystSerializer](d: InternalRow, ordinal: Int): T = {
        val ser = CatalystSerializer[T]
        val struct = d.getStruct(ordinal, ser.schema.size)
        struct.to[T]
      }
      override def create(values: Any*): InternalRow = InternalRow(values: _*)
      override def encode(str: String): UTF8String = UTF8String.fromString(str)
    }
  }

  implicit val envelopeSerializer: CatalystSerializer[Envelope] = new CatalystSerializer[Envelope] {
    override def schema: StructType = StructType(Seq(
      StructField("minX", DoubleType, false),
      StructField("maxX", DoubleType, false),
      StructField("minY", DoubleType, false),
      StructField("maxY", DoubleType, false)
    ))

    override protected def to[R](t: Envelope, io: CatalystIO[R]): R = io.create(
      t.getMinX, t.getMaxX, t.getMinY, t.getMaxX
    )

    override protected def from[R](t: R, io: CatalystIO[R]): Envelope = new Envelope(
      io.getDouble(t, 0), io.getDouble(t, 1), io.getDouble(t, 2), io.getDouble(t, 3)
    )
  }

  implicit val extentSerializer: CatalystSerializer[Extent] = new CatalystSerializer[Extent] {
    override def schema: StructType = StructType(Seq(
      StructField("xmin", DoubleType, false),
      StructField("ymin", DoubleType, false),
      StructField("xmax", DoubleType, false),
      StructField("ymax", DoubleType, false)
    ))
    override def to[R](t: Extent, io: CatalystIO[R]): R = io.create(
      t.xmin, t.ymin, t.xmax, t.ymax
    )
    override def from[R](row: R, io: CatalystIO[R]): Extent = Extent(
      io.getDouble(row, 0), io.getDouble(row, 1), io.getDouble(row, 2), io.getDouble(row, 3)
    )
  }

  implicit val crsSerializer: CatalystSerializer[CRS] = new CatalystSerializer[CRS] {
    override def schema: StructType = StructType(Seq(
      StructField("crsProj4", StringType, false)
    ))
    override def to[R](t: CRS, io: CatalystIO[R]): R = io.create(
      io.encode(t.toProj4String)
    )
    override def from[R](row: R, io: CatalystIO[R]): CRS =
      CRS.fromString(io.getString(row, 0))
  }

  implicit val cellTypeSerializer: CatalystSerializer[CellType] = new CatalystSerializer[CellType] {
    override def schema: StructType = StructType(Seq(
      StructField("cellTypeName", StringType, false)
    ))
    override def to[R](t: CellType, io: CatalystIO[R]): R = io.create(
      io.encode(t.toString())
    )
    override def from[R](row: R, io: CatalystIO[R]): CellType =
      CellType.fromName(io.getString(row, 0))
  }

  implicit val rasterRefSerializer: CatalystSerializer[RasterRef] = new CatalystSerializer[RasterRef] {
    val rsType = new RasterSourceUDT()
    override def schema: StructType = StructType(Seq(
      StructField("source", rsType, false),
      StructField("subextent", apply[Extent].schema, true)
    ))

    override def to[R](t: RasterRef, io: CatalystIO[R]): R = io.create(
      io.to(t.source),
      t.subextent.map(io.to[Extent]).orNull
    )

    override def from[R](row: R, io: CatalystIO[R]): RasterRef = RasterRef(
      io.get[RasterSource](row, 0),
      if (io.isNullAt(row, 1)) None
      else Option(io.get[Extent](row, 1))
    )
  }

  private[rasterframes]
  implicit def tileSerializer: CatalystSerializer[Tile] = TileUDT.tileSerializer
  private[rasterframes]
  implicit def rasterSourceSerializer: CatalystSerializer[RasterSource] = RasterSourceUDT.rasterSourceSerializer

  implicit class WithToRow[T: CatalystSerializer](t: T) {
    def toInternalRow: InternalRow = CatalystSerializer[T].toInternalRow(t)
    def toRow: Row = CatalystSerializer[T].toRow(t)
  }

  implicit class WithFromInternalRow(val r: InternalRow) extends AnyVal {
    def to[T: CatalystSerializer]: T = CatalystSerializer[T].fromInternalRow(r)
  }

  implicit class WithFromRow(val r: Row) extends AnyVal {
    def to[T: CatalystSerializer]: T = CatalystSerializer[T].fromRow(r)
  }
}
