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

package org.locationtech.rasterframes

import geotrellis.raster.resample.Bilinear
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.raster.{IntConstantNoDataCellType, Raster, Tile}
import org.apache.spark.sql.functions._
import org.locationtech.rasterframes.expressions.aggregates.TileRasterizerAggregate
import org.locationtech.rasterframes.expressions.aggregates.TileRasterizerAggregate.ProjectedRasterDefinition
import org.locationtech.rasterframes.model.TileDimensions


class RasterJoinSpec extends TestEnvironment with TestData with RasterMatchers {
  import spark.implicits._
  describe("Raster join between two DataFrames") {
    val s1 = readSingleband("L8-B4-Elkton-VA.tiff")
    // Same data, reprojected to EPSG:4326
    val s2 = readSingleband("L8-B4-Elkton-VA-4326.tiff")

    val r1 = s1.toDF(TileDimensions(10, 10))
    val r2 = s2.toDF(TileDimensions(10, 10))
      .withColumnRenamed("tile", "tile2")

    it("should join the same scene correctly") {
      val r1prime = s1.toDF(TileDimensions(10, 10))
        .withColumnRenamed("tile", "tile2")
      val joined = r1.rasterJoin(r1prime)

      joined.count() should be (r1.count())

      val measure = joined.select(
            rf_tile_mean(rf_local_subtract($"tile", $"tile2")) as "diff_mean",
            rf_tile_stats(rf_local_subtract($"tile", $"tile2")).getField("variance") as "diff_var")
          .collect()
      measure.forall(r ⇒ r.getDouble(0) == 0.0) should be (true)
      measure.forall(r ⇒ r.getDouble(1) == 0.0) should be (true)
    }

    it("should join same scene in two projections, same tile size") {

      // r2 source data is gdal warped r1 data; join them together.
      val joined = r1.rasterJoin(r2)
      // create a Raster from tile2 which should be almost equal to s1
      val result = joined.agg(TileRasterizerAggregate(
        ProjectedRasterDefinition(s1.cols, s1.rows, s1.cellType, s1.crs, s1.extent, Bilinear),
        $"crs", $"extent", $"tile2") as "raster"
      ).select(col("raster").as[Raster[Tile]]).first()

      result.extent shouldBe s1.extent

      // Test the overall local difference of the `result` versus the original
      import geotrellis.raster.mapalgebra.local._
      val sub = s1.extent.buffer(-s1.extent.width * 0.01)
      val diff = Abs(
        Subtract(
          result.crop(sub).tile.convert(IntConstantNoDataCellType),
          s1.raster.crop(sub).tile.convert(IntConstantNoDataCellType)
        )
      )
      // DN's within arbitrary threshold. N.B. the range of values in the source raster is (6396, 27835)
      diff.statisticsDouble.get.mean should be (0.0 +- 200)
      // Overall signal is preserved
      val s1_stddev = s1.tile.statisticsDouble.get.stddev
      val rel_diff = diff.statisticsDouble.get.mean / s1_stddev
      rel_diff should be (0.0 +- 0.15)

      // Use the tile structure of the `joined` dataframe to argue that the structure of the image is similar between `s1` and `joined.tile2`
      val tile_diffs = joined.select((abs(rf_tile_mean($"tile") - rf_tile_mean($"tile2")) / lit(s1_stddev)).alias("z"))

      // Check the 90%-ile z score; recognize there will be some localized areas of larger error
      tile_diffs.selectExpr("percentile(z, 0.90)").as[Double].first() should be < 0.10
      // Check the median z score; it is pretty close to zero
      tile_diffs.selectExpr("percentile(z, 0.50)").as[Double].first() should be < 0.025
     }


  }
}
