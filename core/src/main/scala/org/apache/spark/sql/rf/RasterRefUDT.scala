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

import astraea.spark.rasterframes.ref.RasterRef
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types._

/**
 * Catalyst representation of a tile with extent and CRS.
 *
 * @since 9/5/18
 */
@SQLUserDefinedType(udt = classOf[RasterRefUDT])
class RasterRefUDT extends UserDefinedType[RasterRef] {
  override def typeName = "rf_raster_ref"

  override def pyUDT: String = "pyrasterframes.RasterRefUDT"

  override def sqlType: DataType = RasterRefUDT.schema

  override def serialize(obj: RasterRef): InternalRow =
    Option(obj)
      .map(RasterRefUDT.encode)
      .orNull

  override def deserialize(datum: Any): RasterRef =
    Option(datum)
      .collect {
        case ir: InternalRow ⇒ RasterRefUDT.decode(ir)
      }
      .orNull

  def userClass: Class[RasterRef] = classOf[RasterRef]

  private[sql] override def acceptsType(dataType: DataType) = dataType match {
    case _: RasterRefUDT ⇒ true
    case _ ⇒ super.acceptsType(dataType)
  }
}


object RasterRefUDT extends RasterRefUDT {
  UDTRegistration.register(classOf[RasterRef].getName, classOf[RasterRefUDT].getName)

  private val rrEncoder = Encoders
    .kryo(classOf[RasterRef])
    .asInstanceOf[ExpressionEncoder[RasterRef]]

  def schema = rrEncoder.schema
  def encode(rr: RasterRef): InternalRow = rrEncoder.encode(rr)
  def decode(row: InternalRow): RasterRef = rrEncoder.decode(row)
}