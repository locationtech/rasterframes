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
import geotrellis.proj4.CRS
import org.apache.spark.sql.types.{DataType, _}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.rasterframes.model.LazyCRS
import org.apache.spark.sql.catalyst.InternalRow


@SQLUserDefinedType(udt = classOf[CrsUDT])
class CrsUDT extends UserDefinedType[CRS] {
  override def typeName: String = CrsUDT.typeName

  override def pyUDT: String = "pyrasterframes.rf_types.CrsUDT"

  def userClass: Class[CRS] = classOf[CRS]

  def sqlType: DataType = StringType

  override def serialize(obj: CRS): UTF8String =
    Option(obj)
      .map { crs => UTF8String.fromString(obj.toProj4String) }
      .orNull

  override def deserialize(datum: Any): CRS =
    Option(datum)
      .collect {
        case ir: InternalRow ⇒
          LazyCRS(ir.getString(0))
        case s: UTF8String ⇒
          LazyCRS(s.toString)
      }
      .orNull

  override def acceptsType(dataType: DataType): Boolean = dataType match {
    case _: CrsUDT ⇒ true
    case _ ⇒ super.acceptsType(dataType)
  }
}

case object CrsUDT  {
  UDTRegistration.register(classOf[CRS].getName, classOf[CrsUDT].getName)

  final val typeName: String = "crs"
}
