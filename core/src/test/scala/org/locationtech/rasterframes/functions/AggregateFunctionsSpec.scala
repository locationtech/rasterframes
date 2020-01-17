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

package org.locationtech.rasterframes.functions
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster._
import geotrellis.raster.render.Png
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.vector.Extent
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions._
import org.locationtech.rasterframes.TestData._
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.encoders.StandardEncoders
import org.locationtech.rasterframes.stats._
import org.locationtech.rasterframes.tiles.ProjectedRasterTile
import org.locationtech.rasterframes.tiles.ProjectedRasterTile.prtEncoder

class AggregateFunctionsSpec extends TestEnvironment with RasterMatchers {
  import spark.implicits._

  describe("aggregate statistics") {
    it("should count data cells") {
      val df = randNDTilesWithNull.filter(_ != null).toDF("tile")
      df.select(rf_agg_data_cells($"tile")).first() should be(expectedRandData)
      df.selectExpr("rf_agg_data_cells(tile)").as[Long].first() should be(expectedRandData)

      checkDocs("rf_agg_data_cells")
    }
    it("should count no-data cells") {
      val df = TestData.randNDTilesWithNull.toDF("tile")
      df.select(rf_agg_no_data_cells($"tile")).first() should be(expectedRandNoData)
      df.selectExpr("rf_agg_no_data_cells(tile)").as[Long].first() should be(expectedRandNoData)
      checkDocs("rf_agg_no_data_cells")
    }

    it("should compute aggregate statistics") {
      val df = TestData.randNDTilesWithNull.toDF("tile")

      df.select(rf_agg_stats($"tile") as "stats")
        .select("stats.data_cells", "stats.no_data_cells")
        .as[(Long, Long)]
        .first() should be((expectedRandData, expectedRandNoData))
      df.selectExpr("rf_agg_stats(tile) as stats")
        .select("stats.data_cells")
        .as[Long]
        .first() should be(expectedRandData)

      checkDocs("rf_agg_stats")
    }

    it("should compute a aggregate histogram") {
      val df = TestData.randNDTilesWithNull.toDF("tile")
      val hist1 = df.select(rf_agg_approx_histogram($"tile")).first()
      val hist2 = df
        .selectExpr("rf_agg_approx_histogram(tile) as hist")
        .select($"hist".as[CellHistogram])
        .first()
      hist1 should be(hist2)
      checkDocs("rf_agg_approx_histogram")
    }

    it("should compute local statistics") {
      val df = TestData.randNDTilesWithNull.toDF("tile")
      val stats1 = df
        .select(rf_agg_local_stats($"tile"))
        .first()
      val stats2 = df
        .selectExpr("rf_agg_local_stats(tile) as stats")
        .select($"stats".as[LocalCellStatistics])
        .first()

      stats1 should be(stats2)
      checkDocs("rf_agg_local_stats")
    }

    it("should compute local min") {
      val df = Seq(two, three, one, six).toDF("tile")
      df.select(rf_agg_local_min($"tile")).first() should be(one.toArrayTile())
      df.selectExpr("rf_agg_local_min(tile)").as[Tile].first() should be(one.toArrayTile())
      checkDocs("rf_agg_local_min")
    }

    it("should compute local max") {
      val df = Seq(two, three, one, six).toDF("tile")
      df.select(rf_agg_local_max($"tile")).first() should be(six.toArrayTile())
      df.selectExpr("rf_agg_local_max(tile)").as[Tile].first() should be(six.toArrayTile())
      checkDocs("rf_agg_local_max")
    }

    it("should compute local mean") {
      checkDocs("rf_agg_local_mean")
      val df = Seq(two, three, one, six)
        .toDF("tile")
        .withColumn("id", monotonically_increasing_id())

      df.select(rf_agg_local_mean($"tile")).first() should be(three.toArrayTile())

      df.selectExpr("rf_agg_local_mean(tile)").as[Tile].first() should be(three.toArrayTile())

      noException should be thrownBy {
        df.groupBy($"id")
          .agg(rf_agg_local_mean($"tile"))
          .collect()
      }
    }

    it("should compute local data cell counts") {
      val df = Seq(two, randNDPRT, nd).toDF("tile")
      val t1 = df.select(rf_agg_local_data_cells($"tile")).first()
      val t2 = df.selectExpr("rf_agg_local_data_cells(tile) as cnt").select($"cnt".as[Tile]).first()
      t1 should be(t2)
      checkDocs("rf_agg_local_data_cells")
    }

    it("should compute local no-data cell counts") {
      val df = Seq(two, randNDPRT, nd).toDF("tile")
      val t1 = df.select(rf_agg_local_no_data_cells($"tile")).first()
      val t2 = df.selectExpr("rf_agg_local_no_data_cells(tile) as cnt").select($"cnt".as[Tile]).first()
      t1 should be(t2)
      val t3 = df.select(rf_local_add(rf_agg_local_data_cells($"tile"), rf_agg_local_no_data_cells($"tile"))).as[Tile].first()
      t3 should be(three.toArrayTile())
      checkDocs("rf_agg_local_no_data_cells")
    }
  }

  describe("aggregate rasters") {
    it("should create a global aggregate raster from proj_raster column") {
      implicit val enc = Encoders.tuple(
        StandardEncoders.extentEncoder,
        StandardEncoders.crsSparkEncoder,
        ExpressionEncoder[Tile](),
        ExpressionEncoder[Tile](),
        ExpressionEncoder[Tile]()
      )
      val src = TestData.rgbCogSample
      val extent = src.extent
      val df = src
        .toDF(Dimensions(32, 49))
        .as[(Extent, CRS, Tile, Tile, Tile)]
        .map(p => ProjectedRasterTile(p._3, p._1, p._2))
      val aoi = extent.reproject(src.crs, WebMercator).buffer(-(extent.width * 0.2))
      val overview = df.select(rf_agg_overview_raster($"value", 500, 400, aoi))
      val (min, max) = overview.first().findMinMaxDouble
      val (expectedMin, expectedMax) = src.tile.band(0).findMinMaxDouble
      min should be(expectedMin +- 100)
      max should be(expectedMax +- 100)

      val png = Png(overview.select(rf_render_png(col(overview.columns.head), "Greyscale256")).first())
      png.write("target/agg-raster1.png")
    }

    it("should create a global aggregate raster from separate tile, extent, and crs column") {
      val src = TestData.sampleGeoTiff
      val df = src.toDF(Dimensions(32, 32))
      val extent = src.extent
      val aoi0 = extent.reproject(src.crs, WebMercator)
      val aoi = aoi0.buffer(-(aoi0.width * 0.2))
      val overview = df.select(rf_agg_overview_raster($"tile", $"extent", $"crs", 500, 400, aoi, Bilinear))
      val (min, max) = overview.first().findMinMaxDouble
      val (expectedMin, expectedMax) = src.tile.findMinMaxDouble

      val png = Png(overview.select(rf_render_png(col(overview.columns.head), "Greyscale64")).first())
      png.write("target/agg-raster2.png")

      // It's not exact because we've cut out a section and resampled it.
      min should be(expectedMin +- 2000)
      max should be(expectedMax +- 2000)
    }

    ignore("should work in SQL") {
      val src = TestData.rgbCogSample
      val df = src.toDF(Dimensions(32, 32))
      noException shouldBe thrownBy {
        df.selectExpr("rf_agg_overview_raster(500, 400, aoi, extent, crs, b_1)").as[Tile].first()
      }
    }

    ignore("should have docs") {
      checkDocs("rf_agg_overview_raster")
    }
  }

  describe("geometric aggregates") {
    // SQL docs not available until we re-implement as an expression
    ignore("should have docs") {
      checkDocs("rf_agg_extent")
      checkDocs("rf_agg_reprojected_extent")
    }

    it("should compute an aggregate extent") {
      val src = TestData.l8Sample(1)
      val df = src.toDF(Dimensions(10, 10))
      val result = df.select(rf_agg_extent($"extent")).first()
      result should be(src.extent)
    }

    it("should compute a reprojected aggregate extent") {
      val src = TestData.l8Sample(1)
      val df = src.toDF(Dimensions(10, 10))
      val result = df.select(rf_agg_reprojected_extent($"extent", $"crs", WebMercator)).first()
      result should be(src.extent.reproject(src.crs, WebMercator))
    }
  }
}
