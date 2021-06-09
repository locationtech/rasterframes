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
import geotrellis.layer.{Bounds, KeyBounds, SpatialKey}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, _}
import org.locationtech.rasterframes.encoders.StandardSerializers

//TODO: Is this UDT still needed after refactor switching ot new Aggregation API?
@SQLUserDefinedType(udt = classOf[BoundsUDT])
class BoundsUDT extends UserDefinedType[Bounds[_]] {
  override def typeName: String = BoundsUDT.typeName

  override def pyUDT: String = "pyrasterframes.rf_types.BoundsUDT"

  def userClass: Class[Bounds[_]] = classOf[Bounds[_]]

  def sqlType: DataType = StandardSerializers.boundsSerializer[SpatialKey].schema

  //TODO: handle TemporalKey
  override def serialize(obj: Bounds[_]): InternalRow = {
    val dims = obj.asInstanceOf[KeyBounds[SpatialKey]]
    StandardSerializers.boundsSerializer[SpatialKey].toInternalRow(dims)
  }

  override def deserialize(datum: Any): Bounds[SpatialKey] =
    Option(datum)
      .collect {
        case ir: InternalRow ⇒
          StandardSerializers.boundsSerializer[SpatialKey].fromInternalRow(ir)
      }.orNull

  override def acceptsType(dataType: DataType): Boolean = dataType match {
    case _: BoundsUDT ⇒ true
    case _ ⇒ super.acceptsType(dataType)
  }
}

case object BoundsUDT  {
  UDTRegistration.register(classOf[Bounds[_]].getName, classOf[BoundsUDT].getName)

  final val typeName: String = "key_bounds"
}
