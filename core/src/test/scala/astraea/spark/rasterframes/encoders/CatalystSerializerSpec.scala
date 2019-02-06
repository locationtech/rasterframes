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
import astraea.spark.rasterframes.model.CellContext
import astraea.spark.rasterframes.{TestData, TestEnvironment}
import geotrellis.proj4._
import geotrellis.raster.UShortUserDefinedNoDataCellType
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

class CatalystSerializerSpec extends TestEnvironment with TestData {

  describe("Specialized serialization on specific types") {
    it("should support encoding") {
      implicit val enc: ExpressionEncoder[CRS] = CatalystSerializerEncoder[CRS]

      val values = Seq[CRS](LatLng, Sinusoidal, ConusAlbers, WebMercator)
      val df = spark.createDataset(values)(enc)
      //df.show(false)
      val results = df.collect()
      results should contain allElementsOf values
    }

    it("should serialize CRS") {
      val ser = CatalystSerializer[CRS]
      ser.fromRow(ser.toRow(LatLng)) should be(LatLng)
      ser.fromRow(ser.toRow(Sinusoidal)) should be(Sinusoidal)
    }

    it("should serialize CellContext") {
      val ct = CellContext(UShortUserDefinedNoDataCellType(3), 12, 23)
      val ser = CatalystSerializer[CellContext]
      ser.fromRow(ser.toRow(ct)) should be(ct)
    }
  }
}
