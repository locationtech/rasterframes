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

import java.nio.ByteBuffer

import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, UDTRegistration, UserDefinedType, _}
import org.locationtech.rasterframes.encoders.CatalystSerializer
import org.locationtech.rasterframes.ref.RasterSource
import org.locationtech.rasterframes.util.KryoSupport

/**
 * Catalyst representation of a RasterSource.
 *
 * @since 9/5/18
 */
@SQLUserDefinedType(udt = classOf[RasterSourceUDT])
class RasterSourceUDT extends UserDefinedType[RasterSource] {
  import RasterSourceUDT._
  override def typeName = "rastersource"

  override def pyUDT: String = "pyrasterframes.rf_types.RasterSourceUDT"

  def userClass: Class[RasterSource] = classOf[RasterSource]

  override def sqlType: DataType = schemaOf[RasterSource]

  override def serialize(obj: RasterSource): InternalRow =
    Option(obj)
      .map(_.toInternalRow)
      .orNull

  override def deserialize(datum: Any): RasterSource =
    Option(datum)
      .collect {
        case ir: InternalRow ⇒ ir.to[RasterSource]
      }
      .orNull

  private[sql] override def acceptsType(dataType: DataType) = dataType match {
    case _: RasterSourceUDT ⇒ true
    case _ ⇒ super.acceptsType(dataType)
  }
}

object RasterSourceUDT {
  UDTRegistration.register(classOf[RasterSource].getName, classOf[RasterSourceUDT].getName)

  /** Deserialize a byte array, also used inside the Python API */
  def from(byteArray: Array[Byte]): RasterSource = CatalystSerializer.CatalystIO.rowIO.create(byteArray).to[RasterSource]

  implicit val rasterSourceSerializer: CatalystSerializer[RasterSource] = new CatalystSerializer[RasterSource] {

    override val schema: StructType = StructType(Seq(
      StructField("raster_source_kryo", BinaryType, false)
    ))

    override def to[R](t: RasterSource, io: CatalystIO[R]): R = {
      val buf = KryoSupport.serialize(t)
      io.create(buf.array())
    }

    override def from[R](row: R, io: CatalystIO[R]): RasterSource = {
      KryoSupport.deserialize[RasterSource](ByteBuffer.wrap(io.getByteArray(row, 0)))
    }
  }
}
