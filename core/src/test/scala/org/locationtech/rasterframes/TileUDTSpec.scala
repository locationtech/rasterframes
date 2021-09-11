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

package org.locationtech.rasterframes

import geotrellis.raster
import geotrellis.raster.{CellType, Dimensions, NoNoData, Tile}
import org.apache.spark.sql.types.StringType
import org.locationtech.rasterframes.tiles.ShowableTile
import org.scalatest.Inspectors

/**
 * RasterFrameLayer test rig.
 *
 * @since 7/10/17
 */
class TileUDTSpec extends TestEnvironment with TestData with Inspectors {
  import TestData.randomTile

  spark.version

  describe("TileUDT") {
    val tileSizes = Seq(2, 7, 64, 128, 511)
    val ct = functions.cellTypes().filter(_ != "bool")

    def forEveryConfig(test: Tile => Unit): Unit = {
      forEvery(tileSizes.combinations(2).toSeq) { case Seq(tc, tr) =>
        forEvery(ct) { c =>
          val tile = randomTile(tc, tr, CellType.fromName(c))
          test(tile)
        }
      }
    }

    it("should (de)serialize tile") {
      forEveryConfig { tile =>
        val row = tileUDT.serialize(tile)
        val tileAgain = tileUDT.deserialize(row)
        assert(tileAgain === tile)
      }
    }

    it("should (en/de)code tile") {
      forEveryConfig { tile =>
        val row = tileEncoder.createSerializer().apply(tile)
        assert(!row.isNullAt(0))
        val tileAgain = tileUDT.deserialize(row.getStruct(0, tileUDT.sqlType.size))
        assert(tileAgain === tile)
      }
    }

    it("should extract properties") {
      forEveryConfig { tile =>
        val row = tileUDT.serialize(tile)
        val wrapper = tileUDT.deserialize(row)
        assert(wrapper.cols === tile.cols)
        assert(wrapper.rows === tile.rows)
        assert(wrapper.cellType === tile.cellType)
      }
    }

    it("should directly extract cells") {
      forEveryConfig { tile =>
        val row = tileUDT.serialize(tile)
        val wrapper = tileUDT.deserialize(row)
        val Dimensions(cols,rows) = wrapper.dimensions
        val indexes = Seq((0, 0), (cols - 1, rows - 1), (cols/2, rows/2), (1, 1))
        forAll(indexes) { case (c, r) =>
          assert(wrapper.get(c, r) === tile.get(c, r))
          assert(wrapper.getDouble(c, r) === tile.getDouble(c, r))
        }
      }
    }

    it("should provide a pretty-print tile") {
      import spark.implicits._

      if (rfConfig.getBoolean("showable-tiles"))
        forEveryConfig { tile =>
          val stringified = Seq(tile).toDF("tile").select($"tile".cast(StringType)).as[String].first()
          stringified should be(ShowableTile.show(tile))

          if(!tile.cellType.isInstanceOf[NoNoData]) {
            val withNd = tile.mutable
            withNd.update(0, raster.NODATA)
            ShowableTile.show(withNd) should include("--")
          }
        }
    }
  }
}
