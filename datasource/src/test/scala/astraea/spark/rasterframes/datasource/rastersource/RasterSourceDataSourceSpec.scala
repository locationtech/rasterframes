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
import astraea.spark.rasterframes.datasource.rastersource.RasterSourceDataSource.{PATHS_PARAM, PATH_PARAM}
import astraea.spark.rasterframes._
import astraea.spark.rasterframes.util._

class RasterSourceDataSourceSpec extends TestEnvironment with TestData {
  describe("DataSource parameter processing") {
    it("should handle single `path`") {
      val p = Map(PATH_PARAM -> "/usr/local/foo/bar.tif")
      RasterSourceDataSource.filePaths(p) should be (p.values.toSeq)
    }
    it("should handle single `paths`") {
      val p = Map(PATHS_PARAM -> "/usr/local/foo/bar.tif")
      RasterSourceDataSource.filePaths(p) should be (p.values.toSeq)
    }
    it("should handle multiple `paths`") {
      val expected = Seq("/usr/local/foo/bar.tif", "/usr/local/bar/foo.tif")
      val p = Map(PATHS_PARAM -> expected.mkString("\n\r", "\n\n", "\r"))
      RasterSourceDataSource.filePaths(p) should be (expected)
    }
    it("should handle both `path` and `paths`") {
      val expected1 = Seq("/usr/local/foo/bar.tif", "/usr/local/bar/foo.tif")
      val expected2 = "/usr/local/barf/baz.tif"
      val p = Map(PATHS_PARAM -> expected1.mkString("\n"), PATH_PARAM -> expected2)
      RasterSourceDataSource.filePaths(p) should be (expected1 :+ expected2)
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
        .withBandCount(3)
        .load(cogPath.toASCIIString)
      val tcols = df.tileColumns
      tcols.length should be(3)
      tcols.map(_.columnName) should contain allElementsOf Seq("tile_1", "tile_2", "tile_3")
    }
    it("should read a single file") {
      val df = spark.read.rastersource.load(l8samplePath.toASCIIString)
      df.printSchema()
      df.show(false)
      fail()
    }
    it("should read a multiple files with one band") {
      val df = spark.read.rastersource
        .from(Seq(cogPath, l8samplePath, nonCogPath))
        .load()
      df.printSchema()
      df.show(false)
      fail()
    }

  }
}
