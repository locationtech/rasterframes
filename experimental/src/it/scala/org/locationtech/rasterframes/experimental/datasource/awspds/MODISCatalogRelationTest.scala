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

import geotrellis.proj4.LatLng
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
    it("should compute aggregate statistics") {
      // This is copied from the docs.
      import spark.implicits._

      val modis = spark.read.format("aws-pds-modis-catalog").load()

      val red_nir_monthly_2017 = modis
        .select($"granule_id", month($"acquisition_date") as "month", $"B01" as "red", $"B02" as "nir")
        .where(year($"acquisition_date") === 2017 && (dayofmonth($"acquisition_date") === 15) && $"granule_id" === "h21v09")

      val red_nir_tiles_monthly_2017 = spark.read.raster
        .fromCatalog(red_nir_monthly_2017, "red", "nir")
        .load()
        .cache()

      val result = red_nir_tiles_monthly_2017
        .where(st_intersects(
          st_reproject(rf_geometry($"red"), rf_crs($"red"), LatLng),
          st_makePoint(34.870605, -4.729727)
        ))
        .groupBy("month")
        .agg(rf_agg_stats(rf_normalized_difference($"nir", $"red")) as "ndvi_stats")
        .orderBy("month")
        .select("month", "ndvi_stats.*")

      result.show()
    }
  }
}
