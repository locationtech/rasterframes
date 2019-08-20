/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea. Inc.
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
 *
 */

package org.locationtech.rasterframes.experimental.datasource.awspds
import java.sql.Timestamp

import org.apache.spark.sql.functions._
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.datasource.raster._
import org.locationtech.rasterframes.stats.CellStatistics

/**
 * Test rig for MODIS catalog stuff.
 *
 * @since 5/4/18
 */
class MODISCatalogRelationTest extends TestEnvironment {
  import spark.implicits._
  val catalog = spark.read.modisCatalog.load()
  val scenes = catalog
    .where($"acquisition_date".as[Timestamp] === to_timestamp(lit("2018-1-1")))
    .where($"granule_id".contains("h24v03"))
    .cache()

  describe("Representing MODIS scenes as a Spark data source") {
    it("should provide a non-empty catalog") {
      scenes.count() should be (1)
    }

    it("should provide 7 band and 7 qa urls") {
      scenes.schema.count(_.name.startsWith("B")) should be (14)
    }

    it("should construct valid URLs") {
      val urlStr = scenes.select("B03").as[String].first
      val code = TestSupport.urlResponse(urlStr)
      withClue(urlStr) {
        code should be(200)
      }
    }
  }

  describe("Read MODIS scenes from PDS") {
    it("should be compatible with raster DataSource") {
      val df = spark.read.raster
        .fromCatalog(scenes, "B03")
        .withTileDimensions(128, 128)
        .load()

      // Further refine down to a tile
      val sub = df.select($"B03", st_centroid(st_geometry(rf_extent($"B03"))))
        .where(st_contains(st_geometry(rf_extent($"B03")), st_makePoint(7175787.353582373, 6345530.965564346)))
        .withColumn("stats", rf_tile_stats(rf_tile($"B03")))

      val stats = sub.select($"stats".as[CellStatistics]).first()

      stats.data_cells shouldBe < (128L * 128L)
      stats.data_cells shouldBe > (128L)
      stats.mean shouldBe > (1000.0)
    }
  }
}
