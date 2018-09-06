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

import astraea.spark.rasterframes.encoders.StandardEncoders
import geotrellis.raster.{ProjectedRaster, Tile}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, UDTRegistration, UserDefinedType, _}

/**
 * Catalyst representation of a tile with extent and CRS.
 *
 * @since 9/5/18
 */
@SQLUserDefinedType(udt = classOf[ProjectedRasterUDT])
class ProjectedRasterUDT extends UserDefinedType[ProjectedRaster[Tile]] {
  override def typeName = "rf_raster"

  override def pyUDT: String = "pyrasterframes.ProjectedRasterUDT"

  override def sqlType: DataType = ProjectedRasterUDT.schema

  override def serialize(obj: ProjectedRaster[Tile]): InternalRow =
    Option(obj)
      .map(ProjectedRasterUDT.encode)
      .orNull

  override def deserialize(datum: Any): ProjectedRaster[Tile] =
    Option(datum)
      .collect {
        case ir: InternalRow ⇒ ProjectedRasterUDT.decode(ir)
      }
      .orNull

  def userClass: Class[ProjectedRaster[Tile]] = classOf[ProjectedRaster[Tile]]

  private[sql] override def acceptsType(dataType: DataType) = dataType match {
    case _: ProjectedRasterUDT ⇒ true
    case _ ⇒ super.acceptsType(dataType)
  }
}

object ProjectedRasterUDT extends ProjectedRasterUDT {
  UDTRegistration.register(classOf[ProjectedRaster[Tile]].getName, classOf[ProjectedRasterUDT].getName)
  private val crsEncoder = StandardEncoders.crsEncoder.resolveAndBind()
  private val extentEncoder = StandardEncoders.extentEncoder.resolveAndBind()
  private val tileUDT = new TileUDT()

  val schema = StructType(Seq(
    StructField("crs", crsEncoder.schema, false),
    StructField("extent", extentEncoder.schema, false),
    StructField("tile", tileUDT.sqlType, false)
  ))

  def encode(pr: ProjectedRaster[Tile]): InternalRow = InternalRow(
    crsEncoder.toRow(pr.crs),
    extentEncoder.toRow(pr.extent),
    tileUDT.serialize(pr.tile)
  )
  def decode(row: InternalRow): ProjectedRaster[Tile] = ProjectedRaster(
    tileUDT.deserialize(row.getStruct(2, tileUDT.sqlType.size)),
    extentEncoder.fromRow(row.getStruct(1, extentEncoder.schema.size)),
    crsEncoder.fromRow(row.getStruct(0, crsEncoder.schema.size))
  )
}
