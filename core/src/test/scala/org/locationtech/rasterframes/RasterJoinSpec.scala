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
import geotrellis.vector.Extent
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.locationtech.rasterframes.expressions.aggregates.TileRasterizerAggregate
import org.locationtech.rasterframes.expressions.aggregates.TileRasterizerAggregate.ProjectedRasterDefinition
import org.locationtech.rasterframes.model.TileDimensions


class RasterJoinSpec extends TestEnvironment with TestData with RasterMatchers {
  import spark.implicits._
  describe("Raster join between two DataFrames") {
    val b4nativeTif = readSingleband("L8-B4-Elkton-VA.tiff")
    // Same data, reprojected to EPSG:4326
    val b4warpedTif = readSingleband("L8-B4-Elkton-VA-4326.tiff")

    val b4nativeRf = b4nativeTif.toDF(TileDimensions(10, 10))
    val b4warpedRf = b4warpedTif.toDF(TileDimensions(10, 10))
      .withColumnRenamed("tile", "tile2")

    it("should join the same scene correctly") {

      val b4nativeRfPrime = b4nativeTif.toDF(TileDimensions(10, 10))
        .withColumnRenamed("tile", "tile2")
      val joined = b4nativeRf.rasterJoin(b4nativeRfPrime)

      joined.count() should be (b4nativeRf.count())

      val measure = joined.select(
            rf_tile_mean(rf_local_subtract($"tile", $"tile2")) as "diff_mean",
            rf_tile_stats(rf_local_subtract($"tile", $"tile2")).getField("variance") as "diff_var")
          .as[(Double, Double)]
          .collect()
      all (measure) should be ((0.0, 0.0))
    }

    it("should join same scene in different tile sizes"){
      val r1prime = b4nativeTif.toDF(TileDimensions(25, 25)).withColumnRenamed("tile", "tile2")
      r1prime.select(rf_dimensions($"tile2").getField("rows")).as[Int].first() should be (25)
      val joined = b4nativeRf.rasterJoin(r1prime)

      joined.count() should be (b4nativeRf.count())

      val measure = joined.select(
        rf_tile_mean(rf_local_subtract($"tile", $"tile2")) as "diff_mean",
        rf_tile_stats(rf_local_subtract($"tile", $"tile2")).getField("variance") as "diff_var")
        .as[(Double, Double)]
        .collect()
      all (measure) should be ((0.0, 0.0))

    }

    it("should join same scene in two projections, same tile size") {
      // b4warpedRf source data is gdal warped b4nativeRf data; join them together.
      val joined = b4nativeRf.rasterJoin(b4warpedRf)
      // create a Raster from tile2 which should be almost equal to b4nativeTif
      val agg = joined.agg(TileRasterizerAggregate(
        ProjectedRasterDefinition(b4nativeTif.cols, b4nativeTif.rows, b4nativeTif.cellType, b4nativeTif.crs, b4nativeTif.extent, Bilinear),
        $"crs", $"extent", $"tile2") as "raster"
      ).select(col("raster").as[Raster[Tile]])

      agg.printSchema()
      val result = agg.first()

      result.extent shouldBe b4nativeTif.extent

      // Test the overall local difference of the `result` versus the original
      import geotrellis.raster.mapalgebra.local._
      val sub = b4nativeTif.extent.buffer(-b4nativeTif.extent.width * 0.01)
      val diff = Abs(
        Subtract(
          result.crop(sub).tile.convert(IntConstantNoDataCellType),
          b4nativeTif.raster.crop(sub).tile.convert(IntConstantNoDataCellType)
        )
      )
      // DN's within arbitrary threshold. N.B. the range of values in the source raster is (6396, 27835)
      diff.statisticsDouble.get.mean should be (0.0 +- 200)
      // Overall signal is preserved
      val b4nativeStddev = b4nativeTif.tile.statisticsDouble.get.stddev
      val rel_diff = diff.statisticsDouble.get.mean /  b4nativeStddev
      rel_diff should be (0.0 +- 0.15)

      // Use the tile structure of the `joined` dataframe to argue that the structure of the image is similar between `b4nativeTif` and `joined.tile2`
      val tile_diffs = joined.select((abs(rf_tile_mean($"tile") - rf_tile_mean($"tile2")) / lit( b4nativeStddev)).alias("z"))

      // Check the 90%-ile z score; recognize there will be some localized areas of larger error
      tile_diffs.selectExpr("percentile(z, 0.90)").as[Double].first() should be < 0.10
      // Check the median z score; it is pretty close to zero
      tile_diffs.selectExpr("percentile(z, 0.50)").as[Double].first() should be < 0.025
     }

    it("should join multiple RHS tile columns"){
      // join multiple native CRS bands to the EPSG 4326 RF

      val multibandRf = b4nativeRf
        .withColumn("t_plus", rf_local_add($"tile", $"tile"))
        .withColumn("t_mult", rf_local_multiply($"tile", $"tile"))
      multibandRf.tileColumns.length should be (3)

      val multibandJoin = multibandRf.rasterJoin(b4warpedRf)

      multibandJoin.tileColumns.length should be (4)
      multibandJoin.count() should be (multibandRf.count())
    }

    it("should join with heterogeneous LHS CRS and coverages"){

      val df17 = readSingleband("m_3607824_se_17_1_20160620_subset.tif")
        .toDF(TileDimensions(50, 50))
        .withColumn("utm", lit(17))
      // neighboring and slightly overlapping NAIP scene
      val df18 = readSingleband("m_3607717_sw_18_1_20160620_subset.tif")
        .toDF(TileDimensions(60, 60))
        .withColumn("utm", lit(18))

      df17.count() should be (6 * 6) // file is 300 x 300
      df18.count() should be (5 * 5) // file is 300 x 300

      val df = df17.union(df18)
      df.count() should be (6 * 6 + 5 * 5)
      val expectCrs = Array("+proj=utm +zone=17 +datum=NAD83 +units=m +no_defs ", "+proj=utm +zone=18 +datum=NAD83 +units=m +no_defs ")
      df.select($"crs".getField("crsProj4")).distinct().as[String].collect() should contain theSameElementsAs expectCrs

      // read a third source to join. burned in box that intersects both above subsets; but more so on the df17
      val box = readSingleband("m_3607_box.tif").toDF(TileDimensions(4,4)).withColumnRenamed("tile", "burned")
      val joined = df.rasterJoin(box)

      joined.count() should be (df.count)

      val totals = joined.groupBy($"utm").agg(sum(rf_tile_sum($"burned")).alias("burned_total"))
      val total18 = totals.where($"utm" === 18).select($"burned_total").as[Double].first()
      val total17 = totals.where($"utm" === 17).select($"burned_total").as[Double].first()

      total18 should be > 0.0
      total18 should be < total17


    }

    it("should pass through ancillary columns") {
      val left = b4nativeRf.withColumn("left_id", monotonically_increasing_id())
      val right = b4warpedRf.withColumn("right_id", monotonically_increasing_id())
      val joined = left.rasterJoin(right)
      joined.columns should contain allElementsOf Seq("left_id", "right_id_agg")
    }
  }

  override def additionalConf: SparkConf = super.additionalConf.set("spark.sql.codegen.comments", "true")
}
