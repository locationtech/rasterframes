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

package org.locationtech.rasterframes

import org.scalatest.Inspectors
import geotrellis.proj4.LatLng
import geotrellis.proj4.CRS
import org.locationtech.rasterframes.ref.RFRasterSource
import org.locationtech.rasterframes.ref.RasterRef

class CrsSpec extends TestEnvironment with TestData with Inspectors {
  import spark.implicits._

  describe("CrsUDT") {
    it("should extract from CRS") {
      val df = List(Option(LatLng: CRS)).toDF("crs")
      val crs_df = df.select(rf_crs($"crs"))
      crs_df.take(1).head shouldBe LatLng
    }

    it("should extract from raster") {
      val df = List(Option(one)).toDF("raster")
      val crs_df = df.select(rf_crs($"raster"))
      crs_df.take(1).head shouldBe one.crs
    }

    it("should extract from rastersource") {
      val src = RFRasterSource(remoteMODIS)
      val df = Seq(src).toDF("src")
      val crs_df = df.select(rf_crs($"src"))
      crs_df.take(1).head shouldBe src.crs
    }

    it("should extract from RasterRef") {
      val src = RFRasterSource(remoteCOGSingleband1)
      val ref = RasterRef(src, 0, None, None)
      val df = Seq(Option(ref)).toDF("ref")
      val crs_df = df.select(rf_crs($"ref"))
      crs_df.take(1).head shouldBe ref.crs
    }
  }
}
