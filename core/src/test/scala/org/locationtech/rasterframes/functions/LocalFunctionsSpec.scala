/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2020 Astraea, Inc.
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

package org.locationtech.rasterframes.functions

import org.locationtech.rasterframes.TestEnvironment
import geotrellis.raster._
import geotrellis.raster.testkit.RasterMatchers
import org.apache.spark.sql.functions._
import org.locationtech.rasterframes.expressions.accessors.ExtractTile
import org.locationtech.rasterframes.tiles.ProjectedRasterTile
import org.locationtech.rasterframes._

class LocalFunctionsSpec extends TestEnvironment with RasterMatchers {

  import TestData._
  import spark.implicits._

  describe("arithmetic tile operations") {
    it("should local_add") {
      val df = Seq((one, two)).toDF("one", "two")

      val maybeThree = df.select(rf_local_add($"one", $"two")).as[Option[ProjectedRasterTile]]
      assertEqual(maybeThree.first().get, three)

      assertEqual(df.selectExpr("rf_local_add(one, two) as three").as[Option[ProjectedRasterTile]].first().get, three)

      val maybeThreeTile = df.select(rf_local_add(ExtractTile($"one"), ExtractTile($"two"))).as[Tile]
      assertEqual(maybeThreeTile.first(), three.toArrayTile())
      checkDocs("rf_local_add")
    }

    it("should rf_local_subtract") {
      val df = Seq((three, two)).toDF("three", "two")
      val maybeOne = df.select(rf_local_subtract($"three", $"two").as[ProjectedRasterTile])
      assertEqual(maybeOne.first(), one)

      assertEqual(df.selectExpr("rf_local_subtract(three, two)").as[Option[ProjectedRasterTile]].first().get, one)

      val maybeOneTile =
        df.select(rf_local_subtract(ExtractTile($"three"), ExtractTile($"two"))).as[Tile]
      assertEqual(maybeOneTile.first(), one.toArrayTile())
      checkDocs("rf_local_subtract")
    }

    it("should rf_local_multiply") {
      val df = Seq((three, two)).toDF("three", "two")

      val maybeSix = df.select(rf_local_multiply($"three", $"two").as[ProjectedRasterTile])
      assertEqual(maybeSix.first(), six)

      assertEqual(df.selectExpr("rf_local_multiply(three, two)").as[Option[ProjectedRasterTile]].first().get, six)

      val maybeSixTile =
        df.select(rf_local_multiply(ExtractTile($"three"), ExtractTile($"two"))).as[Tile]
      assertEqual(maybeSixTile.first(), six.toArrayTile())
      checkDocs("rf_local_multiply")
    }

    it("should rf_local_divide") {
      val df = Seq((six, two)).toDF("six", "two")
      val maybeThree = df.select(rf_local_divide($"six", $"two").as[ProjectedRasterTile])
      assertEqual(maybeThree.first(), three)

      assertEqual(df.selectExpr("rf_local_divide(six, two)").as[Option[ProjectedRasterTile]].first().get, three)

      // note: division by constant will promote byte tile to double tile
      assertEqual(df.selectExpr("rf_local_divide(six, 2.0)").as[Option[ProjectedRasterTile]].first().get, three)
      assertEqual(df.selectExpr("rf_local_multiply(rf_local_divide(six, 2.0), two)").as[Option[ProjectedRasterTile]].first().get, six)

      val maybeThreeTile =
        df.select(rf_local_divide(ExtractTile($"six"), ExtractTile($"two"))).as[Tile]
      assertEqual(maybeThreeTile.first(), three.toArrayTile())
      checkDocs("rf_local_divide")
    }
  }

  describe("scalar tile operations") {
    it("should rf_local_add") {
      val df = Seq(Option(one)).toDF("raster")
      val maybeThree = df.select(rf_local_add($"raster", 2).as[ProjectedRasterTile])
      assertEqual(maybeThree.first(), three)

      val maybeThreeD = df.select(rf_local_add($"raster", 2.1).as[ProjectedRasterTile])
      assertEqual(maybeThreeD.first(), three.convert(DoubleConstantNoDataCellType).localAdd(0.1))

      val maybeThreeTile = df.select(rf_local_add(ExtractTile($"raster"), 2)).as[Tile]
      assertEqual(maybeThreeTile.first(), three.toArrayTile())
    }

    it("should rf_local_subtract") {
      val df = Seq((two, three)).toDF("two","three")

      val maybeOne = df.select(rf_local_subtract($"three", 2).as[ProjectedRasterTile])
      assertEqual(maybeOne.first(), one)

      val maybeOneD = df.select(rf_local_subtract($"three", 2.0).as[ProjectedRasterTile])
      assertEqual(maybeOneD.first(), one)

      val maybeOneTile = df.select(rf_local_subtract(ExtractTile($"three"), 2)).as[Tile]
      assertEqual(maybeOneTile.first(), one.toArrayTile())
    }

    it("should rf_local_multiply") {
      val df = Seq((two, three)).toDF("two", "three")

      val maybeSix = df.select(rf_local_multiply($"three", 2).as[ProjectedRasterTile])
      assertEqual(maybeSix.first(), six)

      val maybeSixD = df.select(rf_local_multiply($"three", 2.0).as[ProjectedRasterTile])
      assertEqual(maybeSixD.first(), six)

      val maybeSixTile = df.select(rf_local_multiply(ExtractTile($"three"), 2)).as[Tile]
      assertEqual(maybeSixTile.first(), six.toArrayTile())
    }

    it("should rf_local_divide") {
      val df = Seq((one, six)).toDF("one", "six")

      val maybeThree = df.select(rf_local_divide($"six", 2).as[ProjectedRasterTile])
      assertEqual(maybeThree.first(), three)

      val maybeThreeD = df.select(rf_local_divide($"six", 2.0).as[ProjectedRasterTile])
      assertEqual(maybeThreeD.first(), three)

      val maybeThreeTile = df.select(rf_local_divide(ExtractTile($"six"), 2)).as[Tile]
      assertEqual(maybeThreeTile.first(), three.toArrayTile())
    }
  }

  describe("analytical transformations") {

    it("should return local data and nodata") {
      checkDocs("rf_local_data")
      checkDocs("rf_local_no_data")

      val df = Seq(randNDPRT)
        .toDF("t")
        .withColumn("ld", rf_local_data($"t"))
        .withColumn("lnd", rf_local_no_data($"t"))

      val ndResult = df.select($"lnd").as[Tile].first()
      ndResult should be(randNDPRT.localUndefined())

      val dResult = df.select($"ld").as[Tile].first()
      dResult should be(randNDPRT.localDefined())
    }

    it("should compute rf_normalized_difference") {
      val df = Seq((three, two)).toDF("three", "two")

      df.select(rf_tile_to_array_double(rf_normalized_difference($"three", $"two")))
        .first()
        .forall(_ == 0.2) shouldBe true

      df.selectExpr("rf_tile_to_array_double(rf_normalized_difference(three, two))")
        .as[Array[Double]]
        .first()
        .forall(_ == 0.2) shouldBe true

      checkDocs("rf_normalized_difference")
    }
    it("should round tile cell values") {
      val three_plus = TestData.projectedRasterTile(cols, rows, 3.12, extent, crs, DoubleConstantNoDataCellType)
      val three_less = TestData.projectedRasterTile(cols, rows, 2.92, extent, crs, DoubleConstantNoDataCellType)
      val three_double = TestData.projectedRasterTile(cols, rows, 3.0, extent, crs, DoubleConstantNoDataCellType)

      val df = Seq((three_plus, three_less, three)).toDF("three_plus", "three_less", "three")

      assertEqual(df.select(rf_round($"three").as[ProjectedRasterTile]).first(), three)
      assertEqual(df.select(rf_round($"three_plus").as[ProjectedRasterTile]).first(), three_double)
      assertEqual(df.select(rf_round($"three_less").as[ProjectedRasterTile]).first(), three_double)

      assertEqual(df.selectExpr("rf_round(three)").as[Option[ProjectedRasterTile]].first().get, three)
      assertEqual(df.selectExpr("rf_round(three_plus)").as[Option[ProjectedRasterTile]].first().get, three_double)
      assertEqual(df.selectExpr("rf_round(three_less)").as[Option[ProjectedRasterTile]].first().get, three_double)

      checkDocs("rf_round")
    }

    it("should abs cell values") {
      val minus = one.mapTile(t => t.convert(IntConstantNoDataCellType) * -1)
      val df = Seq((one, minus)).toDF("one", "minus")
      val abs_df = df.select(rf_abs($"minus").as[ProjectedRasterTile])
      assertEqual(abs_df.first(), one)

      checkDocs("rf_abs")
    }

    it("should take logarithms positive cell values") {
      // rf_log10 1000 == 3
      val thousand = TestData.projectedRasterTile(cols, rows, 1000, extent, crs, ShortConstantNoDataCellType)
      val threesDouble = TestData.projectedRasterTile(cols, rows, 3.0, extent, crs, DoubleConstantNoDataCellType)
      val zerosDouble = TestData.projectedRasterTile(cols, rows, 0.0, extent, crs, DoubleConstantNoDataCellType)

      val df1 = Seq((one, thousand)).toDF("one", "tile")
      assertEqual(df1.select(rf_log10($"tile").as[ProjectedRasterTile]).first(), threesDouble)

      // ln random tile == rf_log10 random tile / rf_log10(e); random tile square to ensure all positive cell values
      val df2 = Seq((one, randPositiveDoubleTile)).toDF("one", "tile")
      val log10e = math.log10(math.E)
      assertEqual(
        df2.select(rf_log($"tile").as[ProjectedRasterTile]).first(),
        df2.select(rf_log10($"tile").as[ProjectedRasterTile]).first() / log10e)

      lazy val maybeZeros = df2
        .selectExpr(s"rf_local_subtract(rf_log(tile), rf_local_divide(rf_log10(tile), ${log10e}))")
        .as[Option[ProjectedRasterTile]]
        .first()
      assertEqual(maybeZeros.get, zerosDouble)

      // rf_log1p for zeros should be ln(1)
      val ln1 = math.log1p(0.0)
      val df3 = Seq(Option(zero)).toDF("tile")
      val maybeLn1 = df3.selectExpr(s"rf_log1p(tile)").as[Option[ProjectedRasterTile]].first()
      assert(maybeLn1.get.tile.toArrayDouble().forall(_ == ln1))

      checkDocs("rf_log")
      checkDocs("rf_log2")
      checkDocs("rf_log10")
      checkDocs("rf_log1p")
    }

    it("should take logarithms with non-positive cell values") {
      val ni_float = TestData.projectedRasterTile(cols, rows, Double.NegativeInfinity, extent, crs, DoubleConstantNoDataCellType)
      val zero_float = TestData.projectedRasterTile(cols, rows, 0.0, extent, crs, DoubleConstantNoDataCellType)

      // tile zeros ==> -Infinity
      val df_0 = Seq(Option(zero)).toDF("tile")
      assertEqual(df_0.select(rf_log($"tile").as[ProjectedRasterTile]).first(), ni_float)
      assertEqual(df_0.select(rf_log10($"tile").as[ProjectedRasterTile]).first(), ni_float)
      assertEqual(df_0.select(rf_log2($"tile").as[ProjectedRasterTile]).first(), ni_float)
      // rf_log1p of zeros should be 0.
      assertEqual(df_0.select(rf_log1p($"tile").as[ProjectedRasterTile]).first(), zero_float)

      // tile negative values ==> NaN
      assert(df_0.selectExpr("rf_log(rf_local_subtract(tile, 42))").as[Option[ProjectedRasterTile]].first().get.isNoDataTile)
      assert(df_0.selectExpr("rf_log2(rf_local_subtract(tile, 42))").as[Option[ProjectedRasterTile]].first().get.isNoDataTile)
      assert(df_0.select(rf_log1p(rf_local_subtract($"tile", 42)).as[ProjectedRasterTile]).first().isNoDataTile)
      assert(df_0.select(rf_log10(rf_local_subtract($"tile", lit(0.01))).as[ProjectedRasterTile]).first().isNoDataTile)

    }

    it("should take exponential") {
      val df = Seq(Option(six)).toDF("tile")

      // rf_exp inverses rf_log
      assertEqual(
        df.select(rf_exp(rf_log($"tile")).as[ProjectedRasterTile]).first(),
        six
      )

      // base 2
      assertEqual(df.select(rf_exp2(rf_log2($"tile")).as[ProjectedRasterTile]).first(), six)

      // base 10
      assertEqual(df.select(rf_exp10(rf_log10($"tile")).as[ProjectedRasterTile]).first(), six)

      // plus/minus 1
      assertEqual(df.select(rf_expm1(rf_log1p($"tile")).as[ProjectedRasterTile]).first(), six)

      // SQL
      assertEqual(df.selectExpr("rf_exp(rf_log(tile))").as[Option[ProjectedRasterTile]].first().get, six)

      // SQL base 10
      assertEqual(df.selectExpr("rf_exp10(rf_log10(tile))").as[Option[ProjectedRasterTile]].first().get, six)

      // SQL base 2
      assertEqual(df.selectExpr("rf_exp2(rf_log2(tile))").as[Option[ProjectedRasterTile]].first().get, six)

      // SQL rf_expm1
      assertEqual(df.selectExpr("rf_expm1(rf_log1p(tile)) as res").as[Option[ProjectedRasterTile]].first().get, six)

      checkDocs("rf_exp")
      checkDocs("rf_exp10")
      checkDocs("rf_exp2")
      checkDocs("rf_expm1")

    }

    it("should take square root") {
      checkDocs("rf_sqrt")

      val df = Seq(Option(three)).toDF("tile")
      assertEqual(
        df.select(rf_sqrt(rf_local_multiply($"tile", $"tile")).as[ProjectedRasterTile]).first(),
        three
      )
    }
  }
}