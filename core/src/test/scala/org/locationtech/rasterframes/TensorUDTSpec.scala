/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Azavea, Inc.
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
import geotrellis.raster.{ArrowTensor, CellType, NoNoData, Tile}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.rf._
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.scalatest.Inspectors

/**
  * RasterFrameLayer test rig.
  *
  * @since 11/18/2019
  */
class TensorUDTSpec extends TestEnvironment with TestData with Inspectors {

  spark.version
  val tensorEncoder: ExpressionEncoder[ArrowTensor] = ExpressionEncoder()
  implicit val ser = TensorUDT.tensorSerializer

  describe("TensorUDT") {
    val tileSizes = Seq(2, 7, 64, 128, 511)
    val ct = functions.cellTypes().filter(_ != "bool")

//    it("should (de)serialize tile") {
//      val tensor: ArrowTensor = ArrowTensor.fromArray(
//        Array[Double](1,2,3,4), 2,2)
//      val row = TensorType.serialize(tensor)
//      val tensorAgain = TileType.deserialize(row)
//      assert(tensorAgain === tensor)
//    }
//
    it("should (en/de)code tile") {
      val tensor: ArrowTensor = ArrowTensor.fromArray(
        Array[Double](1,2,3,4), 2,2)
      val row = tensorEncoder.toRow(tensor)
      assert(!row.isNullAt(0))
      val tileAgain = TensorType.deserialize(row.getStruct(0, TensorType.sqlType.size))
      assert(tileAgain === tensor)
    }
//
//    it("should extract properties") {
//      forEveryConfig { tile ⇒
//        val row = TileType.serialize(tile)
//        val wrapper = row.to[Tile]
//        assert(wrapper.cols === tile.cols)
//        assert(wrapper.rows === tile.rows)
//        assert(wrapper.cellType === tile.cellType)
//      }
//    }
//
//    it("should directly extract cells") {
//      forEveryConfig { tile ⇒
//        val row = TileType.serialize(tile)
//        val wrapper = row.to[Tile]
//        val (cols,rows) = wrapper.dimensions
//        val indexes = Seq((0, 0), (cols - 1, rows - 1), (cols/2, rows/2), (1, 1))
//        forAll(indexes) { case (c, r) ⇒
//          assert(wrapper.get(c, r) === tile.get(c, r))
//          assert(wrapper.getDouble(c, r) === tile.getDouble(c, r))
//        }
//      }
//    }
//
//    it("should provide a pretty-print tile") {
//      import spark.implicits._
//      forEveryConfig { tile =>
//        val stringified = Seq(tile).toDF("tile").select($"tile".cast(StringType)).as[String].first()
//        stringified should be(ShowableTile.show(tile))
//
//        if(!tile.cellType.isInstanceOf[NoNoData]) {
//          val withNd = tile.mutable
//          withNd.update(0, raster.NODATA)
//          ShowableTile.show(withNd) should include("--")
//        }
//      }
//    }
  }
}
