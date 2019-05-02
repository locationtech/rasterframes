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

import org.locationtech.rasterframes._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

/**
 * @since 8/21/18
 */
class L8RelationTest extends TestEnvironment with BeforeAndAfterAll with BeforeAndAfter {

  val query =  """
            |SELECT geometry, timestamp, B1, B2
            |FROM l8
            |WHERE
            |  st_intersects(geometry, st_geomFromText('LINESTRING (-39.551 -7.1881, -72.2461 -45.7062)')) AND
            |  timestamp > to_timestamp('2017-11-01') AND
            |  timestamp <= to_timestamp('2017-11-03')
          """.stripMargin

  override protected def beforeAll(): Unit = {
    val l8 = spark.read
      .format(L8DataSource.SHORT_NAME)
      .load()
    l8.createOrReplaceTempView("l8")
    sql(query).createOrReplaceTempView("subscenes")
  }

  describe("Read L8 on PDS as a DataSource") {
    import spark.implicits._
    it("should count scenes") {
      val scenes = sql("SELECT entity_id FROM l8 DISTINCT")
      scenes.count() shouldBe >(300400L)

      val subscenes = sqlContext.table("subscenes")
      subscenes.schema.size should be (4)
      subscenes.count() should be(7)
    }

    it("should compute statistics") {
      val subscenes = sqlContext.table("subscenes")
      val stats = subscenes.select(rf_agg_stats($"B1")).first()
      stats.data_cells shouldBe >(420024000L)
    }
  }
}
