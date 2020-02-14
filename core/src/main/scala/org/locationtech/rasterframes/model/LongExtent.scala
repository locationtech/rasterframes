/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2020 Astraea, Inc.
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

package org.locationtech.rasterframes.model

import geotrellis.vector.Extent
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.locationtech.rasterframes.encoders.CatalystSerializer
import org.locationtech.rasterframes.encoders.CatalystSerializer.CatalystIO

case class LongExtent(xmin: Long, ymin: Long, xmax: Long, ymax: Long) {
  def toExtent: Extent = Extent(xmin.toDouble, ymin.toDouble, xmax.toDouble, ymax.toDouble)
}

object LongExtent {
  implicit val bigIntExtentSerializer: CatalystSerializer[LongExtent] = new CatalystSerializer[LongExtent] {
    override val schema: StructType = StructType(Seq(
      StructField("xmin", LongType, false),
      StructField("ymin", LongType, false),
      StructField("xmax", LongType, false),
      StructField("ymax", LongType, false)
    ))
    override def to[R](t: LongExtent, io: CatalystIO[R]): R = io.create(
      t.xmin, t.ymin, t.xmax, t.ymax
    )
    override def from[R](row: R, io: CatalystIO[R]): LongExtent = LongExtent(
      io.getLong(row, 0),
      io.getLong(row, 1),
      io.getLong(row, 2),
      io.getLong(row, 3)
    )
  }
}
