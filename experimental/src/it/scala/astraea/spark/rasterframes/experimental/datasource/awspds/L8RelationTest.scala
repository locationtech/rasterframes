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

package astraea.spark.rasterframes.experimental.datasource.awspds;

import astraea.spark.rasterframes.TestEnvironment;

/**
 * @since 8/21/18
 */
class L8RelationTest extends TestEnvironment {
  describe("Read L8 on PDS as a DataSource") {
    val l8 = spark.read.format(L8DataSource.SHORT_NAME).load()
    import org.apache.spark.sql.execution.debug._

    it("should count scenes") {
      l8.createOrReplaceTempView("l8")
      val scenes = sql(
        """
          |SELECT bounds, B1, B2
          |FROM l8
          |WHERE
          |  st_intersects(bounds, st_geomFromText('POINT(-76.333 36.985)')) AND
          |  timestamp > to_timestamp('2017-03-12') AND
          |  timestamp <= to_timestamp('2018-01-09')
        """.stripMargin)
      assert(scenes.schema.size === 3)
      assert(scenes.count() === 53)
      scenes.show(false)
    }
  }
}
