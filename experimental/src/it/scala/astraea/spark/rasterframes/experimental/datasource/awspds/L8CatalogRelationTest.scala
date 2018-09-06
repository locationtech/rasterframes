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

import astraea.spark.rasterframes.TestEnvironment
import org.apache.spark.sql.functions._
import astraea.spark.rasterframes.experimental.datasource._

/**
 * Test rig for L8 catalog stuff.
 *
 * @since 5/4/18
 */
class L8CatalogRelationTest extends TestEnvironment {
  describe("Representing L8 scenes as a Spark data source") {
    import spark.implicits._
    val catalog = spark.read.format(L8CatalogDataSource.SHORT_NAME).load()

    val scenes = catalog
      .where($"acquisition_date" === to_timestamp(lit("2017-04-04 15:12:55.394")))
      .where($"path" === 11 && $"row" === 12)

    it("should provide a non-empty catalog") {
      assert(scenes.count() === 1)
    }

    it("should construct band specific download URLs") {
      val b01 = scenes.select(l8_band_url("B1"))
      noException shouldBe thrownBy {
        new URL(b01.first())
      }
    }

    it("should download geotiff as blob") {
      import org.apache.spark.sql.functions.{length ⇒ alength}
      val b01 = scenes.limit(1)
        .select(download(l8_band_url("B1")) as "data")

      val len = b01.select(alength($"data").as[Long])
      assert(len.first() >= 4000000)
    }

    it("should download geotiff as tiles") {
      val b01 = scenes
        .select($"*", download_tiles(l8_band_url("B1")))
      assert(b01.count() === 289)
    }
  }
}
