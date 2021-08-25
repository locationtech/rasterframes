/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Astraea, Inc.
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

package org.locationtech.rasterframes.ml

import geotrellis.proj4.LatLng
import geotrellis.raster.{IntCellType, Tile}
import org.apache.spark.sql.functions.{avg, lit}
import org.locationtech.rasterframes.{TestData, TestEnvironment}
/**
 *
 * @since 2/16/18
 */
class TileExploderSpec extends TestEnvironment with TestData {
  describe("Tile explode transformer") {
    import spark.implicits._
    it("should explode tile") {
      val df = Seq[(Tile, Tile)]((byteArrayTile, byteArrayTile)).toDF("tile1", "tile2").withColumn("other", lit("stuff"))

      val exploder = new TileExploder()
      val newSchema = exploder.transformSchema(df.schema)

      val exploded = exploder.transform(df)

      assert(newSchema === exploded.schema)
      assert(exploded.columns.length === 5)
      assert(exploded.count() === 9)
      write(exploded)
      exploded.agg(avg($"tile1")).as[Double].first() should be (byteArrayTile.statisticsDouble.get.mean)
    }

    it("should explode proj_raster") {
      val randPRT = TestData.projectedRasterTile(10, 10, scala.util.Random.nextInt(), extent, LatLng, IntCellType)

      val df = Seq(Option(randPRT)).toDF("proj_raster").withColumn("other", lit("stuff"))

      val exploder = new TileExploder()
      val newSchema = exploder.transformSchema(df.schema)

      val exploded = exploder.transform(df)

      assert(newSchema === exploded.schema)
      assert(exploded.columns.length === 4)
      assert(exploded.count() === randPRT.size)
      write(exploded)

      exploded.agg(avg($"proj_raster")).as[Double].first() should be (randPRT.statisticsDouble.get.mean)
    }
  }
}
