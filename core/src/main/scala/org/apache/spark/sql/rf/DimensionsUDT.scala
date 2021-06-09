/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2021 Azavea, Inc.
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
import geotrellis.raster.Dimensions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, _}
import org.locationtech.rasterframes.encoders.CatalystSerializer.schemaOf
import org.locationtech.rasterframes.encoders.StandardSerializers


@SQLUserDefinedType(udt = classOf[DimensionsUDT])
class DimensionsUDT extends UserDefinedType[Dimensions[_]] {
  override def typeName: String = DimensionsUDT.typeName

  override def pyUDT: String = "pyrasterframes.rf_types.DimensionsUDT"

  def userClass: Class[Dimensions[_]] = classOf[Dimensions[_]]

  def sqlType: DataType = schemaOf[Dimensions[Int]]

  override def serialize(obj: Dimensions[_]): InternalRow = {
    val dims = obj.asInstanceOf[Dimensions[Int]]
    StandardSerializers.tileDimensionsSerializer.toInternalRow(dims)
  }

  override def deserialize(datum: Any): Dimensions[Int] =
    Option(datum)
      .collect {
        case ir: InternalRow ⇒
          StandardSerializers.tileDimensionsSerializer.fromInternalRow(ir)
      }.orNull

  override def acceptsType(dataType: DataType): Boolean = dataType match {
    case _: DimensionsUDT ⇒ true
    case _ ⇒ super.acceptsType(dataType)
  }
}

case object DimensionsUDT  {
  UDTRegistration.register(classOf[Dimensions[_]].getName, classOf[DimensionsUDT].getName)

  final val typeName: String = "dimensions"
}
