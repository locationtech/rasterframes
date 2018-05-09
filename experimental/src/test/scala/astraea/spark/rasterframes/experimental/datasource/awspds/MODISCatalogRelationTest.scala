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

package astraea.spark.rasterframes.experimental.datasource.awspds
import java.net.URL
import java.sql.Date
import java.time.LocalDate

import astraea.spark.rasterframes.{TestEnvironment, _}


/**
 * Test rig for MODIS catalog stuff.
 *
 * @since 5/4/18
 */
class MODISCatalogRelationTest extends TestEnvironment {
  describe("Representing MODIS scenes as a Spark data source") {
    import spark.implicits._
    val catalog = spark.read.format(MODISCatalogDataSource.NAME).load()
    val scenes = catalog
      .where($"acquisitionDate".as[Date] at LocalDate.of(2018, 1, 1))
      .where($"granuleId".contains("h24v03"))

    it("should provide a non-empty catalog") {
      assert(scenes.count() === 1)
    }

    it("should construct band specific download URLs") {
      val b01 = scenes.select(modis_band_url("B01"))
      b01.show(false)
      noException shouldBe thrownBy {
        new URL(b01.first())
      }
    }

    it("should download geotiff as blob") {
      import org.apache.spark.sql.functions.{length ⇒ alength}
      val b01 = scenes.limit(1)
        .select(download(modis_band_url("B01")) as "data")
        .select(alength($"data").as[Long])
      assert(b01.first() >= 4000000)
    }

    it("should download geotiff as tiles") {
      val b01 = scenes
        .select($"*", download_tiles(modis_band_url("B01")))
      assert(b01.count() === 25)
    }
  }
}
