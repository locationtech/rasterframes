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

package astraea.spark.rasterframes
import astraea.spark.rasterframes.util.SubdivideSupport._
import geotrellis.proj4.LatLng
import geotrellis.raster.{ByteCellType, GridBounds, TileLayout}
import geotrellis.spark.{KeyBounds, SpatialKey, TileLayerMetadata}
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.spark.tiling.CRSWorldExtent

/**
 * Tests miscellaneous extension methods.
 *
 * @since 3/20/18
 */
//noinspection ScalaUnusedSymbol
class ExtensionMethodSpec extends TestEnvironment with TestData {
  lazy val rf = sampleTileLayerRDD.toRF

  describe("DataFrame exention methods") {
    it("should maintain original type") {
      val df = rf.withPrefixedColumnNames("_foo_")
      "val rf2: RasterFrame = df" should compile
    }
    it("should provide tagged column access") {
      val df = rf.drop("tile")
      "val Some(col) = df.spatialKeyColumn" should compile
    }
  }
  describe("RasterFrame exention methods") {
    it("should provide spatial key column") {
      noException should be thrownBy {
        rf.spatialKeyColumn
      }
      "val Some(col) = rf.spatialKeyColumn" shouldNot compile
    }
  }
  describe("Miscellaneous extensions") {
    it("should split TileLayout") {
      val tl1 = TileLayout(2, 3, 10, 10)
      assert(tl1.subdivide(0) === tl1)
      assert(tl1.subdivide(1) === tl1)
      assert(tl1.subdivide(2) === TileLayout(4, 6, 5, 5))
      assertThrows[IllegalArgumentException](tl1.subdivide(-1))

    }
    it("should split KeyBounds[SpatialKey]") {
      val grid = GridBounds(0, 0, 9, 9)
      val kb = KeyBounds(grid)
      val kb2 = kb.subdivide(2)
      assert(kb2.get.toGridBounds() === GridBounds(0, 0, 19, 19))

      val grid2 = GridBounds(2, 2, 9, 9)
      val kb3 = KeyBounds(grid2)
      val kb4 = kb3.subdivide(2)
      assert(kb4.get.toGridBounds() === GridBounds(4, 4, 19, 19))
    }

    it("should split key") {
      val s1 = SpatialKey(0, 0).subdivide(2)
      assert(s1 === Seq(SpatialKey(0,0), SpatialKey(1,0), SpatialKey(0,1), SpatialKey(1,1)))

      val s2 = SpatialKey(2, 3).subdivide(3)
      assert(s2 === Seq(SpatialKey(6,9), SpatialKey(7,9), SpatialKey(8,9), SpatialKey(6,10), SpatialKey(7,10), SpatialKey(8,10), SpatialKey(6,11), SpatialKey(7,11), SpatialKey(8,11)))
    }

    it("should split TileLayerMetadata[SpatialKey]") {
      val tileSize = 12
      val dataGridSize = 2
      val grid = GridBounds(2, 4, 10, 11)
      val layout = LayoutDefinition(LatLng.worldExtent, TileLayout(dataGridSize, dataGridSize, tileSize, tileSize))
      val tlm = TileLayerMetadata(ByteCellType, layout, LatLng.worldExtent, LatLng, KeyBounds(grid))

      val divided = tlm.subdivide(2)

      assert(divided.tileLayout.tileDimensions === (tileSize/2, tileSize/2))
    }
  }
}
