/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2019 Astraea, Inc.
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
import astraea.spark.rasterframes.encoders.CatalystSerializer._
import astraea.spark.rasterframes.expressions.accessors.ExtractTile
import astraea.spark.rasterframes.tiles.ProjectedRasterTile
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.raster.{ByteUserDefinedNoDataCellType, DoubleConstantNoDataCellType, Tile}
import geotrellis.vector.Extent
import org.apache.spark.sql.Encoders
import org.scalatest.{FunSpec, Matchers}

class RasterFunctionsTest extends FunSpec
  with TestEnvironment with Matchers with RasterMatchers {
  import spark.implicits._

  val extent = Extent(10, 20, 30, 40)
  val crs = LatLng
  val ct = ByteUserDefinedNoDataCellType(-2)
  val cols = 10
  val rows = cols
  val one = TestData.projectedRasterTile(cols, rows, 1, extent, crs, ct)
  val two = TestData.projectedRasterTile(cols, rows, 2, extent, crs, ct)
  val three = TestData.projectedRasterTile(cols, rows, 3, extent, crs, ct)
  val six = ProjectedRasterTile(three * two, three.extent, three.crs)
  val nd = TestData.projectedRasterTile(cols, rows, -2, extent, crs, ct)

  implicit val pairEnc = Encoders.tuple(ProjectedRasterTile.prtEncoder, ProjectedRasterTile.prtEncoder)
  implicit val tripEnc = Encoders.tuple(ProjectedRasterTile.prtEncoder, ProjectedRasterTile.prtEncoder, ProjectedRasterTile.prtEncoder)

  describe("arithmetic tile operations") {
    it("should local_add") {
      val df = Seq((one, two)).toDF("one", "two")

      val maybeThree = df.select(local_add($"one", $"two")).as[ProjectedRasterTile]
      assertEqual(maybeThree.first(), three)

      assertEqual(df.selectExpr("rf_local_add(one, two)").as[ProjectedRasterTile].first(), three)

      val maybeThreeTile = df.select(local_add(ExtractTile($"one"), ExtractTile($"two"))).as[Tile]
      assertEqual(maybeThreeTile.first(), three.toArrayTile())
    }

    it("should local_subtract") {
      val df = Seq((three, two)).toDF("three", "two")
      val maybeOne = df.select(local_subtract($"three", $"two")).as[ProjectedRasterTile]
      assertEqual(maybeOne.first(), one)

      assertEqual(df.selectExpr("rf_local_subtract(three, two)").as[ProjectedRasterTile].first(), one)

      val maybeOneTile =
        df.select(local_subtract(ExtractTile($"three"), ExtractTile($"two"))).as[Tile]
      assertEqual(maybeOneTile.first(), one.toArrayTile())
    }

    it("should local_multiply") {
      val df = Seq((three, two)).toDF("three", "two")

      val maybeSix = df.select(local_multiply($"three", $"two")).as[ProjectedRasterTile]
      assertEqual(maybeSix.first(), six)

      assertEqual(df.selectExpr("rf_local_multiply(three, two)").as[ProjectedRasterTile].first(), six)

      val maybeSixTile =
        df.select(local_multiply(ExtractTile($"three"), ExtractTile($"two"))).as[Tile]
      assertEqual(maybeSixTile.first(), six.toArrayTile())
    }

    it("should local_divide") {
      val df = Seq((six, two)).toDF("six", "two")
      val maybeThree = df.select(local_divide($"six", $"two")).as[ProjectedRasterTile]
      assertEqual(maybeThree.first(), three)

      assertEqual(df.selectExpr("rf_local_divide(six, two)").as[ProjectedRasterTile].first(), three)

      val maybeThreeTile =
        df.select(local_divide(ExtractTile($"six"), ExtractTile($"two"))).as[Tile]
      assertEqual(maybeThreeTile.first(), three.toArrayTile())
    }
  }

  describe("scalar tile operations") {
    it("should local_add") {
      val df = Seq(one).toDF("one")
      val maybeThree = df.select(local_add($"one", 2)).as[ProjectedRasterTile]
      assertEqual(maybeThree.first(), three)

      val maybeThreeD = df.select(local_add($"one", 2.1)).as[ProjectedRasterTile]
      assertEqual(maybeThreeD.first(), three.convert(DoubleConstantNoDataCellType).localAdd(0.1))

      val maybeThreeTile = df.select(local_add(ExtractTile($"one"), 2)).as[Tile]
      assertEqual(maybeThreeTile.first(), three.toArrayTile())
    }

    it("should local_subtract") {
      val df = Seq(three).toDF("three")

      val maybeOne = df.select(local_subtract($"three", 2)).as[ProjectedRasterTile]
      assertEqual(maybeOne.first(), one)

      val maybeOneD = df.select(local_subtract($"three", 2.0)).as[ProjectedRasterTile]
      assertEqual(maybeOneD.first(), one)

      val maybeOneTile = df.select(local_subtract(ExtractTile($"three"), 2)).as[Tile]
      assertEqual(maybeOneTile.first(), one.toArrayTile())
    }

    it("should local_multiply") {
      val df = Seq(three).toDF("three")

      val maybeSix = df.select(local_multiply($"three", 2)).as[ProjectedRasterTile]
      assertEqual(maybeSix.first(), six)

      val maybeSixD = df.select(local_multiply($"three", 2.0)).as[ProjectedRasterTile]
      assertEqual(maybeSixD.first(), six)

      val maybeSixTile = df.select(local_multiply(ExtractTile($"three"), 2)).as[Tile]
      assertEqual(maybeSixTile.first(), six.toArrayTile())
    }

    it("should local_divide") {
      val df = Seq(six).toDF("six")

      val maybeThree = df.select(local_divide($"six", 2)).as[ProjectedRasterTile]
      assertEqual(maybeThree.first(), three)

      val maybeThreeD = df.select(local_divide($"six", 2.0)).as[ProjectedRasterTile]
      assertEqual(maybeThreeD.first(), three)

      val maybeThreeTile = df.select(local_divide(ExtractTile($"six"), 2)).as[Tile]
      assertEqual(maybeThreeTile.first(), three.toArrayTile())
    }
  }

  describe("tile comparison relations") {
    it("should evaluate local_less") {
      val df = Seq((two, three, six)).toDF("two", "three", "six")
      df.select(tile_sum(local_less($"two", 6))).first() should be(100.0)
      df.select(tile_sum(local_less($"two", 1.9))).first() should be(0.0)
      df.select(tile_sum(local_less($"two", 2))).first() should be(0.0)
      df.select(tile_sum(local_less($"three", $"two"))).first() should be(0.0)
      df.select(tile_sum(local_less($"three", $"three"))).first() should be(0.0)
      df.select(tile_sum(local_less($"three", $"six"))).first() should be(100.0)

      df.selectExpr("rf_tile_sum(rf_local_less(two, 6))").as[Double].first() should be(100.0)
      df.selectExpr("rf_tile_sum(rf_local_less(three, three))").as[Double].first() should be(0.0)
    }

    it("should evaluate local_less_equal") {
      val df = Seq((two, three, six)).toDF("two", "three", "six")
      df.select(tile_sum(local_less_equal($"two", 6))).first() should be(100.0)
      df.select(tile_sum(local_less_equal($"two", 1.9))).first() should be(0.0)
      df.select(tile_sum(local_less_equal($"two", 2))).first() should be(100.0)
      df.select(tile_sum(local_less_equal($"three", $"two"))).first() should be(0.0)
      df.select(tile_sum(local_less_equal($"three", $"three"))).first() should be(100.0)
      df.select(tile_sum(local_less_equal($"three", $"six"))).first() should be(100.0)

      df.selectExpr("rf_tile_sum(rf_local_less_equal(two, 6))").as[Double].first() should be(100.0)
      df.selectExpr("rf_tile_sum(rf_local_less_equal(three, three))").as[Double].first() should be(100.0)
    }

    it("should evaluate local_greater") {
      val df = Seq((two, three, six)).toDF("two", "three", "six")
      df.select(tile_sum(local_greater($"two", 6))).first() should be(0.0)
      df.select(tile_sum(local_greater($"two", 1.9))).first() should be(100.0)
      df.select(tile_sum(local_greater($"two", 2))).first() should be(0.0)
      df.select(tile_sum(local_greater($"three", $"two"))).first() should be(100.0)
      df.select(tile_sum(local_greater($"three", $"three"))).first() should be(0.0)
      df.select(tile_sum(local_greater($"three", $"six"))).first() should be(0.0)

      df.selectExpr("rf_tile_sum(rf_local_greater(two, 1.9))").as[Double].first() should be(100.0)
      df.selectExpr("rf_tile_sum(rf_local_greater(three, three))").as[Double].first() should be(0.0)
    }

    it("should evaluate local_greater_equal") {
      val df = Seq((two, three, six)).toDF("two", "three", "six")
      df.select(tile_sum(local_greater_equal($"two", 6))).first() should be(0.0)
      df.select(tile_sum(local_greater_equal($"two", 1.9))).first() should be(100.0)
      df.select(tile_sum(local_greater_equal($"two", 2))).first() should be(100.0)
      df.select(tile_sum(local_greater_equal($"three", $"two"))).first() should be(100.0)
      df.select(tile_sum(local_greater_equal($"three", $"three"))).first() should be(100.0)
      df.select(tile_sum(local_greater_equal($"three", $"six"))).first() should be(0.0)
      df.selectExpr("rf_tile_sum(rf_local_greater_equal(two, 1.9))").as[Double].first() should be(100.0)
      df.selectExpr("rf_tile_sum(rf_local_greater_equal(three, three))").as[Double].first() should be(100.0)
    }

    it("should evaluate local_equal") {
      val df = Seq((two, three, three)).toDF("two", "threeA", "threeB")
      df.select(tile_sum(local_equal($"two", 2))).first() should be(100.0)
      df.select(tile_sum(local_equal($"two", 2.1))).first() should be(0.0)
      df.select(tile_sum(local_equal($"two", $"threeA"))).first() should be(0.0)
      df.select(tile_sum(local_equal($"threeA", $"threeB"))).first() should be(100.0)
      df.selectExpr("rf_tile_sum(rf_local_equal(two, 1.9))").as[Double].first() should be(0.0)
      df.selectExpr("rf_tile_sum(rf_local_equal(threeA, threeB))").as[Double].first() should be(100.0)
    }

    it("should evaluate local_unequal") {
      val df = Seq((two, three, three)).toDF("two", "threeA", "threeB")
      df.select(tile_sum(local_unequal($"two", 2))).first() should be(0.0)
      df.select(tile_sum(local_unequal($"two", 2.1))).first() should be(100.0)
      df.select(tile_sum(local_unequal($"two", $"threeA"))).first() should be(100.0)
      df.select(tile_sum(local_unequal($"threeA", $"threeB"))).first() should be(0.0)
      df.selectExpr("rf_tile_sum(rf_local_unequal(two, 1.9))").as[Double].first() should be(100.0)
      df.selectExpr("rf_tile_sum(rf_local_unequal(threeA, threeB))").as[Double].first() should be(0.0)
    }

  }
}