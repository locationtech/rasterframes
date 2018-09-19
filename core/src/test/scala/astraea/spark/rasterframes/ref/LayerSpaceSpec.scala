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
import astraea.spark.rasterframes.tiles.ProjectedRasterTile
import geotrellis.proj4.{CRS, LatLng, Sinusoidal, Transform}
import geotrellis.raster.{GridExtent, Tile, TileLayout}
import geotrellis.raster.render.ascii.AsciiArtEncoder.Palette
import geotrellis.raster.reproject.ReprojectRasterExtent
import geotrellis.raster.resample.ResampleMethod
import geotrellis.spark.tiling.LayoutDefinition
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

  implicit class AsRef(prt: ProjectedRasterTile) {
    def asRef: RasterRef = RasterRef(new InMemoryRasterSource(prt))
  }

  describe("LayerSpace") {
    import spark.implicits._
//    it("should make sense") {
//      val t = t1.reproject(Sinusoidal)
//      println(t1.dimensions, t.dimensions)
//    }

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
      assert(stats.noDataCells < 100) // There should only be a handful of nodata
      //assert(t1.size === stats.dataCells + stats.noDataCells)
      //println(rf.select($"tile".as[Tile]).first().renderAscii(Palette(".*")))
    }

    it("should merge") {
      // UTM zone 32N
      val utm32n = CRS.fromEpsgCode(32632)
      // Full extent in native coords
      val ext32n = Extent(166021.4431, 0.0000, 833978.5569, 9329005.1825)
      // Reproject densified extent to LatLng - note the GridExtent parameters are arbitrary - forced PIXEL_STEP to 50
      val llext32n = ReprojectRasterExtent.reprojectExtent(GridExtent(ext32n, 1.0, 1.0), Transform(utm32n, LatLng))
      val t32n = TestData.projectedRasterTile(100, 100, 32, ext32n, utm32n)
      println(t32n)

      // WGS 84 / UTM zone 33N
      val utm33n = CRS.fromEpsgCode(32633)
      // Full extent in native coords
      val ext33n = Extent(166021.4431, 0.0000, 833978.5569, 9329005.1825)
      // Reproject densified extent to LatLng - note the GridExtent parameters are arbitrary - forces PIXEL_STEP to 50
      val llext33n = ReprojectRasterExtent.reprojectExtent(GridExtent(ext33n, 1.0, 1.0), Transform(utm33n, LatLng))

      val t33n = TestData.projectedRasterTile(100, 100, 33, ext33n, utm33n)
      println(t33n)

      val space = LayerSpace(LatLng, t33n.cellType, LayoutDefinition(
        llext32n.combine(llext33n),
        TileLayout(5, 10, 10, 10)
      ), geotrellis.raster.resample.Average)
      println(space)


      println(ext32n.reproject(utm32n, LatLng))
      println(ext33n.reproject(utm33n, LatLng))

      val df = Seq(t32n.asRef, t33n.asRef).toDF("tile")

      val rf = df.asRF(space)

      rf.printSchema

      rf.show(false)

      println(rf.count())


    }
  }
}
