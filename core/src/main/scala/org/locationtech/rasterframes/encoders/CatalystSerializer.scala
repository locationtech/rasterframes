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

package org.locationtech.rasterframes.encoders

import org.apache.spark.sql.{Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.time.Instant
import scala.collection.mutable

/**
 * Typeclass for converting to/from JVM object to catalyst encoding. The reason this exists is that
 * instantiating and binding `ExpressionEncoder[T]` is *very* expensive, and not suitable for
 * operations internal to an  `Expression`.
 *
 * @since 10/19/18
 */
/*trait CatalystSerializer[T] extends Serializable {
  def schema: StructType
  protected def to[R](t: T, io: CatalystIO[R]): R
  protected def from[R](t: R, io: CatalystIO[R]): T

  final def toRow(t: T): Row = to(t, CatalystIO[Row])
  final def fromRow(row: Row): T = from(row, CatalystIO[Row])

  final def toInternalRow(t: T): InternalRow = to(t, CatalystIO[InternalRow])
  final def fromInternalRow(row: InternalRow): T = from(row, CatalystIO[InternalRow])
}*/

object CatalystSerializer extends StandardSerializers {
  /*def apply[T: CatalystSerializer]: CatalystSerializer[T] = implicitly

  def schemaOf[T: CatalystSerializer]: StructType = apply[T].schema

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
    def toSeq[T: CatalystSerializer](t: Seq[T]): AnyRef
    def get[T >: Null: CatalystSerializer](d: R, ordinal: Int): T
    def getSeq[T >: Null: CatalystSerializer](d: R, ordinal: Int): Seq[T]
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
    def getArray[T >: Null](d: R, ordinal: Int): Array[T]
    def getMap[K >: Null, V >: Null](d: R, ordinal: Int): Map[K, V]
    def getInstant(d: R, ordinal: Int): Instant
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
      override def getByteArray(d: R, ordinal: Int): Array[Byte] =
        d.get(ordinal).asInstanceOf[Array[Byte]]
      override def get[T >: Null: CatalystSerializer](d: R, ordinal: Int): T = {
        d.getAs[Any](ordinal) match {
          case r: Row => r.to[T]
          case o => o.asInstanceOf[T]
        }
      }
      override def toSeq[T: CatalystSerializer](t: Seq[T]): AnyRef = t.map(_.toRow)
      override def getSeq[T >: Null: CatalystSerializer](d: R, ordinal: Int): Seq[T] =
        d.getSeq[Row](ordinal).map(_.to[T])
      override def encode(str: String): String = str

      def getArray[T >: Null](d: R, ordinal: Int): Array[T] = d.get(ordinal).asInstanceOf[Array[T]]
      def getMap[K >: Null, V >: Null](d: R, ordinal: Int): Map[K, V] = d.get(ordinal).asInstanceOf[Map[K, V]]
      def getInstant(d: R, ordinal: Int): Instant = d.getInstant(ordinal)
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
      override def get[T >: Null: CatalystSerializer](d: InternalRow, ordinal: Int): T = {
        val ser = CatalystSerializer[T]
        val struct = d.getStruct(ordinal, ser.schema.size)
        struct.to[T]
      }
      override def create(values: Any*): InternalRow = InternalRow(values: _*)
      override def toSeq[T: CatalystSerializer](t: Seq[T]): ArrayData =
        ArrayData.toArrayData(t.map(_.toInternalRow).toArray)

      override def getSeq[T >: Null: CatalystSerializer](d: InternalRow, ordinal: Int): Seq[T] = {
        val ad = d.getArray(ordinal)
        val result = Array.ofDim[Any](ad.numElements()).asInstanceOf[Array[T]]
        ad.foreach(
          CatalystSerializer[T].schema,
          (i, v) => result(i) = v.asInstanceOf[InternalRow].to[T]
        )
        result.toSeq
      }
      override def encode(str: String): UTF8String = UTF8String.fromString(str)

      def getArray[T >: Null](d: InternalRow, ordinal: Int): Array[T] = d.getArray(ordinal).array.asInstanceOf[Array[T]]

      def getMap[K >: Null, V >: Null](d: InternalRow, ordinal: Int): Map[K, V] = {
        val md = d.getMap(ordinal)
        val kd = md.keyArray().array
        val vd = md.valueArray().array
        val result: mutable.Map[Any, Any] = mutable.Map.empty

        (0 until md.numElements()).map { idx => result.put(kd(idx), vd(idx)) }

        result.toMap.asInstanceOf[Map[K, V]]
      }

      def getInstant(d: InternalRow, ordinal: Int): Instant = d.get(ordinal, TimestampType).asInstanceOf[Instant]
    }
  }

  implicit class WithToRow[T: CatalystSerializer](t: T) {
    def toInternalRow: InternalRow = if (t == null) null else CatalystSerializer[T].toInternalRow(t)
    def toRow: Row = if (t == null) null else CatalystSerializer[T].toRow(t)
  }

  implicit class WithFromInternalRow(val r: InternalRow) extends AnyVal {
    def to[T >: Null: CatalystSerializer]: T = if (r == null) null else CatalystSerializer[T].fromInternalRow(r)
  }

  implicit class WithFromRow(val r: Row) extends AnyVal {
    def to[T >: Null: CatalystSerializer]: T = if (r == null) null else CatalystSerializer[T].fromRow(r)
  }

  implicit class WithTypeConformity(val left: DataType) extends AnyVal {
    def conformsTo[T >: Null: CatalystSerializer]: Boolean =
      org.apache.spark.sql.rf.WithTypeConformity(left).conformsTo(schemaOf[T])
  }*/

  implicit class WithTypeConformityToEncoder(val left: DataType) extends AnyVal {
    def conformsToSchema[A](schema: StructType): Boolean = {
      org.apache.spark.sql.rf.WithTypeConformity(left).conformsTo(schema)
    }
  }
}
