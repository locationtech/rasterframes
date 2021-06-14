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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, _}
import org.locationtech.rasterframes.encoders.CatalystSerializer._


@SQLUserDefinedType(udt = classOf[CrsUDT])
class CrsUDT extends UserDefinedType[CRS] {
  override def typeName: String = CrsUDT.typeName

  override def pyUDT: String = "pyrasterframes.rf_types.CrsUDT"

  def userClass: Class[CRS] = classOf[CRS]

  def sqlType: DataType = schemaOf[CRS]

  override def serialize(obj: CRS): InternalRow = {
    Option(obj)
      .map(_.toInternalRow)
      .orNull
  }

  override def deserialize(datum: Any): CRS =
    Option(datum)
      .collect {
        case ir: InternalRow ⇒ ir.to[CRS]
      }.orNull

  override def acceptsType(dataType: DataType): Boolean = dataType match {
    case _: CrsUDT ⇒ true
    case _ ⇒ super.acceptsType(dataType)
  }
}

case object CrsUDT  {
  UDTRegistration.register(classOf[CRS].getName, classOf[CrsUDT].getName)

  final val typeName: String = "crs"
}
