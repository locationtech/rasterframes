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

package org.locationtech.rasterframes

import org.locationtech.rasterframes.RasterFunctions
import org.apache.spark.sql.functions.{col, explode}

class RasterFramesStatsSpec extends TestEnvironment with TestData {

  import spark.implicits._

  val df = TestData.sampleGeoTiff
    .toDF()
    .withColumn("tilePlus2", rf_local_add(col("tile"), 2))


  describe("Tile quantiles through built-in functions") {

    it("should compute approx percentiles for a single tile col") {
      // Use "explode"
      val result = df
        .select(rf_explode_tiles($"tile"))
        .stat
        .approxQuantile("tile", Array(0.10, 0.50, 0.90), 0.00001)

      result.length should be(3)

      // computing externally with numpy we arrive at 7963, 10068, 12160 for these quantiles
      result should contain inOrderOnly(7963.0, 10068.0, 12160.0)

      // Use "to_array" and built-in explode
      val result2 = df
        .select(explode(rf_tile_to_array_double($"tile")) as "tile")
        .stat
        .approxQuantile("tile", Array(0.10, 0.50, 0.90), 0.00001)

      result2.length should be(3)

      // computing externally with numpy we arrive at 7963, 10068, 12160 for these quantiles
      result2 should contain inOrderOnly(7963.0, 10068.0, 12160.0)

    }
  }

  describe("Tile quantiles through custom aggregate") {
    it("should compute approx percentiles for a single tile col") {
      val result = df
        .select(rf_agg_approx_quantiles($"tile", Seq(0.1, 0.5, 0.9)))
        .first()

      result.length should be(3)

      // computing externally with numpy we arrive at 7963, 10068, 12160 for these quantiles
      result should contain inOrderOnly(7963.0, 10068.0, 12160.0)
    }

  }
}

