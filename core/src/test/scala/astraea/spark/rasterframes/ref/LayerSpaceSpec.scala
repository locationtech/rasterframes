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

package astraea.spark.rasterframes.ref

import astraea.spark.rasterframes._
import astraea.spark.rasterframes.ref.RasterSource.InMemoryRasterSource
import astraea.spark.rasterframes.stats.CellStatistics
import geotrellis.proj4.{LatLng, Sinusoidal, WebMercator}
import geotrellis.raster.Tile
import geotrellis.vector.Extent

/**
 *
 *
 * @since 9/10/18
 */
class LayerSpaceSpec extends TestEnvironment {
  val ext = Extent(1, 2, 13, 12)
  lazy val t1 = TestData.projectedRasterTile(100, 95, 1, ext)
  lazy val s1 = new InMemoryRasterSource(t1)
  lazy val ref1 = RasterRef(s1)


  describe("LayerSpace") {
    import spark.implicits._
    it("should make sense") {
      val t = t1.reproject(Sinusoidal)
      println(t1.dimensions, t.dimensions)
    }

    it("should preserve sturcture in identity case") {
      val space = LayerSpace.from(s1)

      val df = Seq(ref1).toDF("tile")
      val rf = df.asRF(space)
      assert(rf.count() === 1)
      val stats = rf.select(aggStats($"tile")).first()
      assert(stats === CellStatistics(t1.size, 0L, 1.0, 1.0, 1.0, 0.0))

      val t = rf.select($"tile".as[Tile]).first()
      tilesEqual(t, t1)
    }

    it("should preserve values in crs change") {
      val space = LayerSpace.from(s1).reproject(Sinusoidal)

      val df = Seq(ref1).toDF("tile")
      val rf = df.asRF(space)
      assert(rf.count() === 1)
      val stats = rf.select(aggStats($"tile")).first()
      assert((stats.min, stats.max, stats.mean) === (1.0, 1.0, 1.0))
      assert(stats.dataCells > stats.noDataCells) // There should only be a handful
      assert(t1.size === stats.dataCells + stats.noDataCells)
    }
  }
}
