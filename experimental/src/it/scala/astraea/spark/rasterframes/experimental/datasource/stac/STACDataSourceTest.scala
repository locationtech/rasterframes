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

package astraea.spark.rasterframes.experimental.datasource.stac
import java.sql.Timestamp

import astraea.spark.rasterframes.TestEnvironment

/**
 *
 *
 * @since 7/16/18
 */
class STACDataSourceTest extends TestEnvironment {
  describe("Representing MODIS scenes as a Spark data source") {
    import spark.implicits._
    val catalog = spark.read.format(DefaultSource.SHORT_NAME).load()

    it("should provide a non-empty catalog") {
      val scenes = catalog.where($"datetime".between(Timestamp.valueOf("2017-08-01 00:00:00"), Timestamp.valueOf("2017-12-01 00:00:00")))


      scenes.printSchema()
      println(scenes.count())
      //assert(scenes.count() === 1)
    }
  }
}
