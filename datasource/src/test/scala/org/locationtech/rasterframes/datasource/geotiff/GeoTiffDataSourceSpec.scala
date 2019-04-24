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
package org.locationtech.rasterframes.datasource.geotiff

import java.nio.file.Paths

import org.locationtech.rasterframes._
import org.apache.spark.sql.functions._
import org.locationtech.rasterframes.TestEnvironment

/**
 * @since 1/14/18
 */
class GeoTiffDataSourceSpec
    extends TestEnvironment with TestData {

  describe("GeoTiff reading") {

    it("should read sample GeoTiff") {
      val rf = spark.read
        .geotiff
        .loadRF(cogPath)

      assert(rf.count() > 10)
    }

    it("should lay out tiles correctly"){

      val rf = spark.read
        .geotiff
        .loadRF(cogPath)

      val tlm = rf.tileLayerMetadata.left.get
      val gb = tlm.gridBounds
      assert(gb.colMax > gb.colMin)
      assert(gb.rowMax > gb.rowMin)
    }

    it("should lay out tiles correctly for non-tiled tif") {
      val rf = spark.read
        .geotiff
        .loadRF(nonCogPath)

      println(rf.count())
      rf.show(false)

      assert(rf.count() > 1)

      import org.apache.spark.sql.functions._
      logger.info(
        rf.agg(
          min(col("spatial_key.row")) as "rowmin",
          max(col("spatial_key.row")) as "rowmax",
          min(col("spatial_key.col")) as "colmin",
          max(col("spatial_key.col")) as "colmax"

        ).first.toSeq.toString()
      )
      val tlm = rf.tileLayerMetadata.left.get
      val gb = tlm.gridBounds
      assert(gb.rowMax > gb.rowMin)
      assert(gb.colMax > gb.colMin)

    }

    it("should read in correctly check-summed contents") {
      // c.f. TileStatsSpec -> computing statistics over tiles -> should compute tile statistics -> sum
      val rf = spark.read.geotiff.loadRF(l8samplePath)
      val expected = 309149454 // computed with rasterio
      val result = rf.agg(
        sum(rf_tile_sum(rf("tile")))
      ).collect().head.getDouble(0)

      assert(result === expected)
    }

    it("should write GeoTIFF RF to parquet") {
      val rf = spark.read
        .geotiff
        .loadRF(cogPath)
      assert(write(rf))
    }

    it("should write GeoTIFF") {
      val rf = spark.read
        .geotiff
        .loadRF(cogPath)

      logger.info(s"Read extent: ${rf.tileLayerMetadata.merge.extent}")

      val out = Paths.get("target", "example-geotiff.tiff")
      logger.info(s"Writing to $out")
      noException shouldBe thrownBy {
        rf.write.geotiff.save(out.toString)
      }
    }
  }
}
