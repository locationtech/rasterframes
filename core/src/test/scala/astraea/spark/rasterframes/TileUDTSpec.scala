

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
 */

package astraea.spark.rasterframes

import geotrellis.raster.{CellType, Tile}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.scalatest.Inspectors
import astraea.spark.rasterframes.functions.cellTypes
import org.apache.spark.sql.rf._

/**
 * RasterFrame test rig.
 *
 * @since 7/10/17
 */
class TileUDTSpec extends TestEnvironment with TestData with Inspectors {
  import TestData.randomTile

  spark.version
  val tileEncoder: ExpressionEncoder[Tile] = ExpressionEncoder()

  describe("TileUDT") {
    val tileSizes = Seq(2, 64, 128, 222, 511)
    val ct = cellTypes().filter(_ != "bool")

    def forEveryConfig(test: Tile ⇒ Unit): Unit = {
      forEvery(tileSizes.combinations(2).toSeq) { case Seq(cols, rows) ⇒
        forEvery(ct) { c ⇒
          val tile = randomTile(cols, rows, CellType.fromName(c))
          test(tile)
        }
      }
    }

    it("should (de)serialize tile") {
      forEveryConfig { tile ⇒
        val row = TileUDT.serialize(tile)
        val tileAgain = TileUDT.deserialize(row)
        assert(tileAgain === tile)
      }
    }

    it("should (en/de)code tile") {
      forEveryConfig { tile ⇒
        val row = tileEncoder.encode(tile)
        assert(!row.isNullAt(0))
        val tileAgain = TileUDT.deserialize(row.getStruct(0, TileUDT.sqlType.size))
        assert(tileAgain === tile)
      }
    }

    it("should extract properties") {
      forEveryConfig { tile ⇒
        val row = TileUDT.serialize(tile)
        val wrapper = TileUDT.decode(row)
        assert(wrapper.cols === tile.cols)
        assert(wrapper.rows === tile.rows)
        assert(wrapper.cellType === tile.cellType)
      }
    }

    it("should directly extract cells") {
      forEveryConfig { tile ⇒
        val row = TileUDT.serialize(tile)
        val wrapper = TileUDT.decode(row)
        val (cols,rows) = wrapper.dimensions
        val indexes = Seq((0, 0), (cols - 1, rows - 1), (cols/2, rows/2), (1, 1))
        forAll(indexes) { case (c, r) ⇒
          assert(wrapper.get(c, r) === tile.get(c, r))
          assert(wrapper.getDouble(c, r) === tile.getDouble(c, r))
        }
      }
    }
  }
}
