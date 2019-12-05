/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Azavea, Inc.
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
import org.locationtech.rasterframes.ref.RasterRef.RasterRefTile
import org.locationtech.rasterframes.tiles.InternalRowTile


/**
  * UDT for singleband tiles.
  *
  * @since 11/18/19
  */
@SQLUserDefinedType(udt = classOf[TensorUDT])
class TensorUDT extends UserDefinedType[ArrowTensor] {
  import TensorUDT._
  override def typeName = TensorUDT.typeName

  override def pyUDT: String = "pyrasterframes.rf_types.TensorUDT"

  def userClass: Class[ArrowTensor] = classOf[ArrowTensor]

  def sqlType: StructType = schemaOf[ArrowTensor]

  override def serialize(obj: ArrowTensor): InternalRow =
    Option(obj)
      .map(_.toInternalRow)
      .orNull

  override def deserialize(datum: Any): ArrowTensor =
    Option(datum)
      .collect {
        case ir: InternalRow ⇒ ir.to[ArrowTensor]
      }
      .orNull

  override def acceptsType(dataType: DataType): Boolean = dataType match {
    case _: TensorUDT ⇒ true
    case _ ⇒ super.acceptsType(dataType)
  }
}

case object TensorUDT  {
  UDTRegistration.register(classOf[ArrowTensor].getName, classOf[TensorUDT].getName)

  final val typeName: String = "tensor"

  implicit def tensorSerializer: CatalystSerializer[ArrowTensor] = new CatalystSerializer[ArrowTensor] {

    override val schema: StructType = StructType(Seq(
      StructField("arrow_tensor", BinaryType, true)
    ))

    override def to[R](t: ArrowTensor, io: CatalystIO[R]): R = io.create(
      // TODO: encode ArrowTensor into bytes and Byte array here
      Array.empty[Byte]
    )

    override def from[R](row: R, io: CatalystIO[R]): ArrowTensor = {
      val bytes = io.getByteArray(row, 0)
      ArrowTensor.fromArrowMessage(bytes)
      // TODO: decode the arrow buffer here
      ???
    }
  }
}
