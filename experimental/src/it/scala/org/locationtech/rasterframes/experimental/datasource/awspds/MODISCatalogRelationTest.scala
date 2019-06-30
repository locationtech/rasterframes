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
import java.net.URL
import java.sql.Timestamp

import org.apache.spark.sql.functions._
import org.locationtech.rasterframes.TestEnvironment

/**
 * Test rig for MODIS catalog stuff.
 *
 * @since 5/4/18
 */
class MODISCatalogRelationTest extends TestEnvironment {
  describe("Representing MODIS scenes as a Spark data source") {
    import spark.implicits._
    val catalog = spark.read.format(MODISCatalogDataSource.SHORT_NAME).load()
    val scenes = catalog
      .where($"acquisition_date".as[Timestamp] === to_timestamp(lit("2018-1-1")))
      .where($"granule_id".contains("h24v03"))
      .cache()

    it("should provide a non-empty catalog") {
      assert(scenes.count() === 1)
    }

    it("should construct band specific download URLs") {
      val b01 = scenes.select($"assets"("B01").as[String])
      noException shouldBe thrownBy {
        new URL(b01.first())
      }
    }
  }
}
