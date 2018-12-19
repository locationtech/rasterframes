/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea, Inc.
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

package astraea.spark.rasterframes
import astraea.spark.rasterframes.ref.RasterSource
import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster._
import geotrellis.raster.render.ColorRamps
import org.apache.spark.sql.Dataset

/**
 *
 *
 * @since 2018-12-18
 */
class TileAssemblerSpec extends TestEnvironment with TestData {
  import TileAssemblerSpec._
  describe("TileAssembler") {
    import sqlContext.implicits._

    it("should reassemble a realistic scene") {
      val df = util.time("read scene") {
        RasterSource(remoteMODIS).readAll().left.get
          .zipWithIndex
          .map { case (r, i) ⇒ (i, r.tile, r.extent) }
          .toDF("index", "tile", "extent")
          .repartition($"index")
          .forceCache
      }

      val exploded = util.time("exploded") {
        df
          .select($"index", explodeTiles($"tile"))
          .forceCache
      }

      df.unpersist()

      val assembled = util.time("assembled") {
        exploded.printSchema()
        exploded
          .groupBy($"index")
          .agg(assembleTile(COLUMN_INDEX_COLUMN, ROW_INDEX_COLUMN, $"tile", 256, 256, UShortConstantNoDataCellType))
          .forceCache
      }

      exploded.unpersist()

      //assembled.select($"index".as[Int], $"tile".as[Tile]).foreach(p ⇒ p._2.renderPng(ColorRamps.BlueToOrange).write(s"target/${p._1}.png"))

      assert(assembled.count() === df.count())

      val expected = df.select(aggStats($"tile")).first()
      val result = assembled.select(aggStats($"tile")).first()

      assert(result === expected)

    }
  }
}

object TileAssemblerSpec extends  LazyLogging {

  implicit class WithFC[R](val ds: Dataset[R]) extends AnyVal {
    def forceCache: Dataset[R] = {
      val cached  = ds.cache()
      val cnt = cached.count()
      logger.info(s"Caching Dataset ${ds.rdd.id} with size $cnt.")
      cached
    }
  }
}
