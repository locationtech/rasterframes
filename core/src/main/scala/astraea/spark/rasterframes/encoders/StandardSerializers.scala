/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2019 Astraea, Inc.
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

package astraea.spark.rasterframes.encoders
import astraea.spark.rasterframes.encoders.CatalystSerializer.CatalystIO
import astraea.spark.rasterframes.util.CRSParser
import com.vividsolutions.jts.geom.Envelope
import geotrellis.proj4.CRS
import geotrellis.raster.CellType
import geotrellis.vector.Extent
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

/** Collection of CatalystSerializers for third-party types. */
trait StandardSerializers {

  implicit val envelopeSerializer: CatalystSerializer[Envelope] = new CatalystSerializer[Envelope] {
    override def schema: StructType = StructType(Seq(
      StructField("minX", DoubleType, false),
      StructField("maxX", DoubleType, false),
      StructField("minY", DoubleType, false),
      StructField("maxY", DoubleType, false)
    ))

    override protected def to[R](t: Envelope, io: CatalystIO[R]): R = io.create(
      t.getMinX, t.getMaxX, t.getMinY, t.getMaxX
    )

    override protected def from[R](t: R, io: CatalystIO[R]): Envelope = new Envelope(
      io.getDouble(t, 0), io.getDouble(t, 1), io.getDouble(t, 2), io.getDouble(t, 3)
    )
  }

  implicit val extentSerializer: CatalystSerializer[Extent] = new CatalystSerializer[Extent] {
    override def schema: StructType = StructType(Seq(
      StructField("xmin", DoubleType, false),
      StructField("ymin", DoubleType, false),
      StructField("xmax", DoubleType, false),
      StructField("ymax", DoubleType, false)
    ))
    override def to[R](t: Extent, io: CatalystIO[R]): R = io.create(
      t.xmin, t.ymin, t.xmax, t.ymax
    )
    override def from[R](row: R, io: CatalystIO[R]): Extent = Extent(
      io.getDouble(row, 0), io.getDouble(row, 1), io.getDouble(row, 2), io.getDouble(row, 3)
    )
  }

  implicit val crsSerializer: CatalystSerializer[CRS] = new CatalystSerializer[CRS] {
    override def schema: StructType = StructType(Seq(
      StructField("crsProj4", StringType, false)
    ))
    override def to[R](t: CRS, io: CatalystIO[R]): R = io.create(
      io.encode(
        // Don't do this... it's 1000x slower to decode.
        //t.epsgCode.map(c => "EPSG:" + c).getOrElse(t.toProj4String)
        t.toProj4String
      )
    )
    override def from[R](row: R, io: CatalystIO[R]): CRS =
      CRSParser(io.getString(row, 0))
  }

  implicit val cellTypeSerializer: CatalystSerializer[CellType] = new CatalystSerializer[CellType] {
    override def schema: StructType = StructType(Seq(
      StructField("cellTypeName", StringType, false)
    ))
    override def to[R](t: CellType, io: CatalystIO[R]): R = io.create(
      io.encode(t.toString())
    )
    override def from[R](row: R, io: CatalystIO[R]): CellType =
      CellType.fromName(io.getString(row, 0))
  }

}
