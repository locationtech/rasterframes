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
import geotrellis.raster.{CellType, DataType => gtDataType}
import org.apache.spark.sql.types.{DataType, _}
import org.apache.spark.unsafe.types.UTF8String


@SQLUserDefinedType(udt = classOf[CellTypeUDT])
class CellTypeUDT extends UserDefinedType[gtDataType] {
  override def typeName: String = CellTypeUDT.typeName

  // TODO: Implement CellTypeUDT in python
  override def pyUDT: String = "pyrasterframes.rf_types.CellTypeUDT"

  def userClass: Class[gtDataType] = classOf[gtDataType]

  def sqlType: DataType = StringType

  override def serialize(obj: gtDataType): UTF8String =
    UTF8String.fromString(obj.toString())


  override def deserialize(datum: Any): CellType =
    Option(datum)
      .collect {
        case s: UTF8String ⇒ try {
          CellType.fromName(s.toString)
        }
      }
      .orNull

  override def acceptsType(dataType: DataType): Boolean = dataType match {
    case _: CellTypeUDT ⇒ true
    case _ ⇒ super.acceptsType(dataType)
  }
}

case object CellTypeUDT  {
  UDTRegistration.register(classOf[gtDataType].getName, classOf[CellTypeUDT].getName)

  final val typeName: String = "cell_type"
}
