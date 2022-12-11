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

import geotrellis.proj4.CRS
import geotrellis.raster.resample._
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.raster.{Dimensions, IntConstantNoDataCellType, Raster, Tile}
import geotrellis.vector.Extent
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.locationtech.rasterframes.expressions.aggregates.TileRasterizerAggregate
import org.locationtech.rasterframes.expressions.aggregates.TileRasterizerAggregate.ProjectedRasterDefinition


class RasterJoinSpec extends TestEnvironment with TestData with RasterMatchers {
  import spark.implicits._
  describe("Raster join between two DataFrames") {
    val b4nativeTif = readSingleband("L8-B4-Elkton-VA.tiff")
    // Same data, reprojected to EPSG:4326
    val b4warpedTif = readSingleband("L8-B4-Elkton-VA-4326.tiff")

    val b4nativeRf = b4nativeTif.toDF(Dimensions(10, 10))
    val b4warpedRf = b4warpedTif.toDF(Dimensions(10, 10))
      .withColumnRenamed("tile", "tile2")

    it("should join the same scene correctly") {
      val b4nativeRfPrime = b4nativeTif.toDF(Dimensions(10, 10))
        .withColumnRenamed("tile", "tile2")
      val joined = b4nativeRf.rasterJoin(b4nativeRfPrime.hint("broadcast"))

      joined.count() should be (b4nativeRf.count())

      val measure = joined.select(
            rf_tile_mean(rf_local_subtract($"tile", $"tile2")) as "diff_mean",
            rf_tile_stats(rf_local_subtract($"tile", $"tile2")).getField("variance") as "diff_var")
          .as[(Double, Double)]
          .collect()
      all (measure) should be ((0.0, 0.0))
    }

    it("should join same scene in different tile sizes"){
      val r1prime = b4nativeTif.toDF(Dimensions(25, 25)).withColumnRenamed("tile", "tile2")
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
      val srcExtent = b4nativeTif.extent
      // b4warpedRf source data is gdal warped b4nativeRf data; join them together.
      val joined = b4nativeRf.rasterJoin(b4warpedRf)
      // create a Raster from tile2 which should be almost equal to b4nativeTif
      val agg = joined.agg(TileRasterizerAggregate(
        ProjectedRasterDefinition(b4nativeTif.cols, b4nativeTif.rows, b4nativeTif.cellType, b4nativeTif.crs, b4nativeTif.extent, Bilinear),
        $"tile2", $"extent", $"crs") as "raster"
      ).select(col("raster").as[Tile])

      val raster = Raster(agg.first(), srcExtent)

      // Test the overall local difference of the `result` versus the original
      import geotrellis.raster.mapalgebra.local._
      val sub = b4nativeTif.extent.buffer(-b4nativeTif.extent.width * 0.01)
      val diff = Abs(
        Subtract(
          raster.crop(sub).tile.convert(IntConstantNoDataCellType),
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
        .toDF(Dimensions(50, 50))
        .withColumn("utm", lit(17))
      // neighboring and slightly overlapping NAIP scene
      val df18 = readSingleband("m_3607717_sw_18_1_20160620_subset.tif")
        .toDF(Dimensions(60, 60))
        .withColumn("utm", lit(18))

      df17.count() should be (6 * 6) // file is 300 x 300
      df18.count() should be (5 * 5) // file is 300 x 300

      val df = df17.union(df18)
      df.count() should be (6 * 6 + 5 * 5)
      val expectCrs = Array("+proj=utm +zone=17 +datum=NAD83 +units=m +no_defs ", "+proj=utm +zone=18 +datum=NAD83 +units=m +no_defs ")
      df.select($"crs").distinct().as[CRS].collect().map(_.toProj4String) should contain theSameElementsAs expectCrs

      // read a third source to join. burned in box that intersects both above subsets; but more so on the df17
      val box = readSingleband("m_3607_box.tif").toDF(Dimensions(4,4)).withColumnRenamed("tile", "burned")
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

    it("should handle proj_raster types") {
      val df1 = Seq(Option(one)).toDF("one")
      val df2 = Seq(Option(two)).toDF("two")
      noException shouldBe thrownBy {
        val joined1 = df1.rasterJoin(df2)
        val joined2 = df2.rasterJoin(df1)
      }
    }

    it("should raster join multiple times on projected raster"){
      val df0 = Seq(Option(one)).toDF("proj_raster")
      val result = df0.select($"proj_raster" as "t1")
        .rasterJoin(df0.select($"proj_raster" as "t2"))
        .rasterJoin(df0.select($"proj_raster" as "t3"))

      result.tileColumns.length should be (3)
      result.count() should be (1)
    }

    it("should honor resampling options") {
      // test case. replicate existing test condition and check that resampling option results in different output
      val filterExpr = st_intersects(rf_geometry($"tile"), st_point(704940.0, 4251130.0))
      val result = b4nativeRf.rasterJoin(b4warpedRf.withColumnRenamed("tile2", "nearest"), NearestNeighbor)
        .rasterJoin(b4warpedRf.withColumnRenamed("tile2", "CubicSpline"), CubicSpline)
        .withColumn("diff", rf_local_subtract($"nearest", $"cubicSpline"))
        .agg(rf_agg_stats($"diff") as "stats")
        .select($"stats.min" as "min", $"stats.max" as "max")
        .first()

      // This just tests that the tiles are not identical
      result.getAs[Double]("min") should be > (0.0)
    }

    // Failed to execute user defined function(package$$$Lambda$4417/0x00000008019e2840: (struct<xmax:double,xmin:double,ymax:double,ymin:double>, string, array<struct<cellType:string,cols:int,rows:int,cells:binary,ref:struct<source:struct<raster_source_kryo:binary>,bandIndex:int,subextent:struct<xmin:double,ymin:double,xmax:double,ymax:double>,subgrid:struct<colMin:int,rowMin:int,colMax:int,rowMax:int>>>>, array<struct<xmax:double,xmin:double,ymax:double,ymin:double>>, array<string>, struct<cols:int,rows:int>, string) => struct<cellType:string,cols:int,rows:int,cells:binary,ref:struct<source:struct<raster_source_kryo:binary>,bandIndex:int,subextent:struct<xmin:double,ymin:double,xmax:double,ymax:double>,subgrid:struct<colMin:int,rowMin:int,colMax:int,rowMax:int>>>)

    it("should raster join with null left head") {
      // https://github.com/locationtech/rasterframes/issues/462
      val prt = TestData.projectedRasterTile(
        10, 10, 1,
        Extent(0.0, 0.0, 40.0, 40.0),
        CRS.fromEpsgCode(32611),
      )

      val left = Seq(
        (1, "a", prt.tile, prt.tile, prt.extent, prt.crs),
        (1, "b", null, prt.tile, prt.extent, prt.crs)
      ).toDF("i", "j", "t", "u", "e", "c")

      val right = Seq(
        (1, prt.tile, prt.extent, prt.crs)
      ).toDF("i", "r", "e", "c")

      val joined = left.rasterJoin(right,
        left("i") === right("i"),
        left("e"), left("c"),
        right("e"), right("c"),
        NearestNeighbor
      )
      joined.count() should be (2)

      // In the case where the head column is null it will be passed thru
      val t1 = joined
        .select(isnull($"t"))
        .filter($"j" === "b")
        .first()

      t1.getBoolean(0) should be(true)

      // The right hand side tile should get dimensions from col `u` however
      val collected = joined.select(rf_dimensions($"r")).collect()
      collected.headOption should be (Some(Dimensions(10, 10)))

      // If there is no non-null tile on the LHS then the RHS is ill defined
      val joinedNoLeftTile = left
        .drop($"u")
        .rasterJoin(right,
          left("i") === right("i"),
          left("e"), left("c"),
          right("e"), right("c"),
          NearestNeighbor
        )
      joinedNoLeftTile.count() should be (2)

      // If there is no non-null tile on the LHS then the RHS is ill defined
      val t2 = joinedNoLeftTile
        .select(isnull($"t"))
        .filter($"j" === "b")
        .first()
      t2.getBoolean(0) should be(true)

      // Because no non-null tile col on Left side, the right side is null too
      val t3 = joinedNoLeftTile
        .select(isnull($"r"))
        .filter($"j" === "b")
        .first()
      t3.getBoolean(0) should be(true)
    }

  }

  override def additionalConf: SparkConf = super.additionalConf.set("spark.sql.codegen.comments", "true")
}
