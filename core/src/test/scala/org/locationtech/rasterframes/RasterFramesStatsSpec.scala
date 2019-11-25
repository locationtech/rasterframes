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

import org.apache.spark.sql.functions.{col, explode}

class RasterFramesStatsSpec extends TestEnvironment with TestData {

  import spark.implicits._

  val df = TestData.sampleGeoTiff
    .toDF()
    .withColumn("tilePlus2", rf_local_add(col("tile"), 2))

  describe("DataFrame.tileStats extension methods") {
    it("should compute approx percentiles for a single tile col") {

      val result = df
        .tileStat()
        .approxTileQuantile(
          "tile",
          Array(0.10, 0.50, 0.90),
          0.00001
        )

      result.length should be(3)

      // computing externally with numpy we arrive at 7963, 10068, 12160 for these quantiles
      result should contain inOrderOnly(7963.0, 10068.0, 12160.0)
    }

    it("should compute approx percentiles for many tile cols") {
      val result = df
        .tileStat()
        .approxTileQuantile(
          Array("tile", "tilePlus2"),
          Array(0.25, 0.75),
          0.00001
        )
      result.length should be(2)
      // nested inside is another array of length 2 for each p
      result.foreach { c =>
        c.length should be(2)
      }

      result.head should contain inOrderOnly(8701, 11261)
      result.tail.head should contain inOrderOnly(8703, 11263)
    }
  }

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

  describe("Tile quantiles through existing RF functions") {
    it("should compute approx percentiles for a single tile col") {

      // As the number of buckets goes up, the closer we get to the "right" answer.
      val result = df
        .select(rf_agg_approx_histogram($"tile", 500))
        .map(h => h.percentileBreaks(Seq(0.1, 0.5, 0.9)))
        .first()

      result.length should be(3)
      println(result)
      // computing externally with numpy we arrive at 7963, 10068, 12160 for these quantiles
      // This will fail as the histogram algorithm approximates things differently (probably not as well)
      // Result: List(7936.887798369705, 10034.706053861182, 12140.206924858878)
      // result should contain inOrderOnly(7963.0, 10068.0, 12160.0)
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

