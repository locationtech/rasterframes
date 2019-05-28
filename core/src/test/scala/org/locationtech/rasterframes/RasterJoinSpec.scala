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

import geotrellis.raster.io.geotiff.GeoTiff
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
    val s2 = readSingleband("L8-B4-Elkton-VA-4326.tiff")

    val r1 = s1.toDF(TileDimensions(10, 10))
    val r2 = s2.toDF(TileDimensions(10, 10))
      .withColumnRenamed("tile", "tile2")

    it("should join the same scene correctly") {
      val r1prime = s1.toDF(TileDimensions(10, 10))
        .withColumnRenamed("tile", "tile2")
      val joined = r1.rasterJoin(r1prime)
      joined.count() should be (r1.count())

      val measure = joined
        .select(rf_tile_mean(rf_local_subtract($"tile", $"tile2")) as "mean")
        .agg(sum($"mean"))
        .as[Double]
        .first()
      measure should be(0.0)
    }

    it("should join same scene in two projections, same tile size") {
      val joined = r1.rasterJoin(r2)

      val result = joined.agg(TileRasterizerAggregate(
        ProjectedRasterDefinition(s1.cols, s1.rows, s1.cellType, s1.crs, s1.extent, Bilinear),
        $"crs", $"extent", $"tile2") as "raster"
      ).select(col("raster").as[Raster[Tile]]).first()

      //GeoTiff(result, s1.crs).write("target/out.tiff")
      result.extent shouldBe s1.extent

      // Not sure what the right test is... here's... something?
      import geotrellis.raster.mapalgebra.local._
      val sub = s1.extent.buffer(-s1.extent.width * 0.01)
      val diff = Abs(
        Subtract(
          result.crop(sub).tile.convert(IntConstantNoDataCellType),
          s1.raster.crop(sub).tile.convert(IntConstantNoDataCellType)
        )
      )
      diff.statisticsDouble.get.mean should be (0.0 +- 200)
      //GeoTiff(diff, s1.extent, s1.crs).write("target/diff.tiff")
     }
  }
}
