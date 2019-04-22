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

package astraea.spark.rasterframes.datasource.rastersource
import astraea.spark.rasterframes.datasource.rastersource.RasterSourceDataSource._
import astraea.spark.rasterframes._
import astraea.spark.rasterframes.model.TileDimensions
import astraea.spark.rasterframes.util._

class RasterSourceDataSourceSpec extends TestEnvironment with TestData {
  import spark.implicits._

  describe("DataSource parameter processing") {
    it("should handle single `path`") {
      val p = Map(PATH_PARAM -> "/usr/local/foo/bar.tif")
      p.filePaths should be (p.values.toSeq)
    }

    it("should handle single `paths`") {
      val p = Map(PATHS_PARAM -> "/usr/local/foo/bar.tif")
      p.filePaths should be (p.values.toSeq)
    }
    it("should handle multiple `paths`") {
      val expected = Seq("/usr/local/foo/bar.tif", "/usr/local/bar/foo.tif")
      val p = Map(PATHS_PARAM -> expected.mkString("\n\r", "\n\n", "\r"))
      p.filePaths should be (expected)
    }
    it("should handle both `path` and `paths`") {
      val expected1 = Seq("/usr/local/foo/bar.tif", "/usr/local/bar/foo.tif")
      val expected2 = "/usr/local/barf/baz.tif"
      val p = Map(PATHS_PARAM -> expected1.mkString("\n"), PATH_PARAM -> expected2)
      p.filePaths should be (expected1 :+ expected2)
    }
    it("should parse tile dimensions") {
      val p = Map(TILE_DIMS_PARAM -> "4, 5")
      p.tileDims should be (Some(TileDimensions(4, 5)))
    }
  }

  describe("RasterSource as relation reading") {
    it("should default to a single band schema") {
      val df = spark.read.rastersource.load(l8samplePath.toASCIIString)
      val tcols = df.tileColumns
      tcols.length should be(1)
      tcols.map(_.columnName) should contain("tile")
    }
    it("should support a multiband schema") {
      val df = spark.read
        .rastersource
        .withBandIndexes(0, 1, 2)
        .load(cogPath.toASCIIString)
      val tcols = df.tileColumns
      tcols.length should be(3)
      tcols.map(_.columnName) should contain allElementsOf Seq("tile_b0", "tile_b1", "tile_b2")
    }
    it("should read a multiband file") {
      val df = spark.read
        .rastersource
        .withBandIndexes(0, 1, 2)
        .load(cogPath.toASCIIString)
        .cache()
      df.schema.size should be (4)
      // Test (roughly) we have three distinct but compabible bands
      val stats = df.agg(agg_stats($"tile_b0") as "s0", agg_stats($"tile_b1") as "s1", agg_stats($"tile_b2") as "s2")
      stats.select($"s0.data_cells" === $"s1.data_cells").as[Boolean].first() should be(true)
      stats.select($"s0.data_cells" === $"s2.data_cells").as[Boolean].first() should be(true)
      stats.select($"s0.mean" =!= $"s1.mean").as[Boolean].first() should be(true)
      stats.select($"s0.mean" =!= $"s2.mean").as[Boolean].first() should be(true)
    }
    it("should read a single file") {
      // Image is 1028 x 989 -> 9 x 8 tiles
      val df = spark.read.rastersource
        .withTileDimensions(128, 128)
        .load(cogPath.toASCIIString)

      df.count() should be(math.ceil(1028.0 / 128).toInt * math.ceil(989.0 / 128).toInt)

      val dims = df.select(tile_dimensions($"tile").as[TileDimensions]).distinct().collect()
      dims should contain allElementsOf
        Seq(TileDimensions(4,128), TileDimensions(128,128), TileDimensions(128,93), TileDimensions(4,93))

      df.select("path").distinct().count() should be(1)
    }
    it("should read a multiple files with one band") {
      val df = spark.read.rastersource
        .from(Seq(cogPath, l8samplePath, nonCogPath))
        .withTileDimensions(128, 128)
        .load()
      df.select("path").distinct().count() should be(3)
      df.schema.size should be(2)
    }

    it("should read a multiple files with heterogeneous bands") {
      val df = spark.read.rastersource
        .from(Seq(cogPath, l8samplePath, nonCogPath))
        .withTileDimensions(128, 128)
        .withBandIndexes(0, 1, 2, 3)
        .load()
        .cache()
      df.select("path").distinct().count() should be(3)
      df.schema.size should be(5)

      df.select($"tile_b0").count() should be (df.select($"tile_b0").na.drop.count())
      df.select($"tile_b1").na.drop.count() shouldBe <(df.count())
      df.select($"tile_b1").na.drop.count() should be (df.select($"tile_b2").na.drop.count())
      df.select($"tile_b3").na.drop.count() should be (0)
    }
  }
}
