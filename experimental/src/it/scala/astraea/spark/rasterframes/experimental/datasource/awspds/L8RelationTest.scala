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

import astraea.spark.rasterframes._
import astraea.spark.rasterframes.experimental.datasource._
import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

/**
 * @since 8/21/18
 */
class L8RelationTest extends TestEnvironment with BeforeAndAfterAll with BeforeAndAfter {

  private var scenes: DataFrame = _

  val query =  """
            |SELECT bounds, timestamp, B1, B2
            |FROM l8
            |WHERE
            |  st_intersects(bounds, st_geomFromText('LINESTRING (-39.551 -7.1881, -72.2461 -45.7062)')) AND
            |  timestamp > to_timestamp('2017-11-01') AND
            |  timestamp <= to_timestamp('2017-11-03')
          """.stripMargin

  override protected def beforeAll(): Unit = {
    val l8 = spark.read
      .format(L8DataSource.SHORT_NAME)
      .option(L8DataSource.ACCUMULATORS, true)
      .load()
    l8.createOrReplaceTempView("l8")
    scenes = sql(query).cache()
  }

  after {
    ReadAccumulator.log()
  }

  describe("Read L8 on PDS as a DataSource") {

    it("should count scenes") {
      assert(scenes.schema.size === 4)
      assert(scenes.count() === 7)
    }

    it("should count tiles") {
      import spark.implicits._
      val scenesCount = scenes.count()
      val l8 = spark.read
        .format(L8DataSource.SHORT_NAME)
        .option(L8DataSource.ACCUMULATORS, true)
        .option(L8DataSource.USE_TILING, true)
        .load()
      l8.createOrReplaceTempView("l82")
      val scenes2 = sql(query.replaceAll("l8", "l82")).cache()
      val scenes2Count = scenes2.count()
      println(scenesCount, scenes2Count)
    }
  }
}
