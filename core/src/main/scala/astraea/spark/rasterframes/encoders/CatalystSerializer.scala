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

import astraea.spark.rasterframes.ref.{RasterRef, RasterSource}
import com.vividsolutions.jts.geom.Envelope
import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, Tile}
import geotrellis.vector.Extent
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
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
trait CatalystSerializer[T] {
  def schema: StructType
  def toRow(t: T): InternalRow
  def fromRow(row: InternalRow): T
  def fromRow(row: InternalRow, ordinal: Int): T =
    fromRow(row.getStruct(ordinal, schema.length))
}

object CatalystSerializer {
  def apply[T: CatalystSerializer]: CatalystSerializer[T] = implicitly[CatalystSerializer[T]]


  /** Constructs a Dataframe literal from anything with a serializer. */
  def serialized_literal[T >: Null: CatalystSerializer](t: T): Column = {
    val ser = CatalystSerializer[T]
    new Column(Literal.create(ser.toRow(t), ser.schema))
  }

  implicit val envelopeSerializer: CatalystSerializer[Envelope] = new CatalystSerializer[Envelope] {
    override def schema: StructType = StructType(Seq(
      StructField("minX", DoubleType, false),
      StructField("maxX", DoubleType, false),
      StructField("minY", DoubleType, false),
      StructField("maxY", DoubleType, false)
    ))
    override def toRow(t: Envelope): InternalRow = InternalRow(
      t.getMinX, t.getMaxX, t.getMinY, t.getMaxX
    )
    override def fromRow(row: InternalRow): Envelope = new Envelope(
      row.getDouble(0), row.getDouble(1), row.getDouble(2), row.getDouble(3)
    )
  }

  implicit val extentSerializer: CatalystSerializer[Extent] = new CatalystSerializer[Extent] {
    override def schema: StructType = StructType(Seq(
      StructField("xmin", DoubleType, false),
      StructField("ymin", DoubleType, false),
      StructField("xmax", DoubleType, false),
      StructField("ymax", DoubleType, false)
    ))
    override def toRow(t: Extent): InternalRow = InternalRow(
      t.xmin, t.ymin, t.xmax, t.ymax
    )
    override def fromRow(row: InternalRow): Extent = Extent(
      row.getDouble(0),row.getDouble(1),row.getDouble(2),row.getDouble(3)
    )
  }

  implicit val crsSerializer: CatalystSerializer[CRS] = new CatalystSerializer[CRS] {
    override def schema: StructType = StructType(Seq(
      StructField("crsProj4", StringType, false)
    ))
    override def toRow(t: CRS): InternalRow = InternalRow(
      UTF8String.fromString(t.toProj4String)
    )
    override def fromRow(row: InternalRow): CRS =
      CRS.fromString(row.getString(0))
  }

  implicit val cellTypeSerializer: CatalystSerializer[CellType] = new CatalystSerializer[CellType] {
    override def schema: StructType = StructType(Seq(
      StructField("cellTypeName", StringType, false)
    ))
    override def toRow(t: CellType): InternalRow = InternalRow(
      UTF8String.fromString(t.toString())
    )
    override def fromRow(row: InternalRow): CellType = CellType.fromName(row.getString(0))
  }

  implicit val rasterRefSerializer: CatalystSerializer[RasterRef] = new CatalystSerializer[RasterRef] {
    val rsType = new RasterSourceUDT()
    override def schema: StructType = StructType(Seq(
      StructField("source", rsType, false),
      StructField("subextent", apply[Extent].schema, true)
    ))

    override def toRow(t: RasterRef): InternalRow = InternalRow(
      rsType.serialize(t.source),
      t.subextent.map(_.toRow).orNull
    )

    override def fromRow(row: InternalRow): RasterRef = RasterRef(
      rsType.deserialize(row.get(0, rsType)),
      if (row.isNullAt(1)) None else Option(row.to[Extent](1))
    )
  }

  private[rasterframes]
  implicit def tileSerializer: CatalystSerializer[Tile] = TileUDT.tileSerializer
  private[rasterframes]
  implicit def rasterSourceSerializer: CatalystSerializer[RasterSource] = RasterSourceUDT.rasterSourceSerializer

  implicit class WithSchema[T: CatalystSerializer](t: Class[T]) {
    def schema: StructType = CatalystSerializer[T].schema
  }

  implicit class WithToRow[T: CatalystSerializer](t: T) {
    def toRow: InternalRow = CatalystSerializer[T].toRow(t)
  }

  implicit class WithFromRow(val r: InternalRow) extends AnyVal {
    def to[T: CatalystSerializer]: T = CatalystSerializer[T].fromRow(r)
    def to[T: CatalystSerializer](ordinal: Int): T = CatalystSerializer[T].fromRow(r, ordinal)
  }

}
