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

package org.locationtech.rasterframes

import geotrellis.raster._
import geotrellis.raster.testkit.RasterMatchers
import org.apache.spark.sql.functions._
import org.locationtech.rasterframes.expressions.accessors.ExtractTile
import org.locationtech.rasterframes.tiles.ProjectedRasterTile

class RasterFunctionsSpec extends TestEnvironment with RasterMatchers {
  import TestData._
  import spark.implicits._

  describe("arithmetic tile operations") {
    it("should local_add") {
      val df = Seq((one, two)).toDF("one", "two")

      val maybeThree = df.select(rf_local_add($"one", $"two")).as[ProjectedRasterTile]
      assertEqual(maybeThree.first(), three)

      assertEqual(df.selectExpr("rf_local_add(one, two)").as[ProjectedRasterTile].first(), three)

      val maybeThreeTile = df.select(rf_local_add(ExtractTile($"one"), ExtractTile($"two"))).as[Tile]
      assertEqual(maybeThreeTile.first(), three.toArrayTile())
      checkDocs("rf_local_add")
    }

    it("should rf_local_subtract") {
      val df = Seq((three, two)).toDF("three", "two")
      val maybeOne = df.select(rf_local_subtract($"three", $"two")).as[ProjectedRasterTile]
      assertEqual(maybeOne.first(), one)

      assertEqual(df.selectExpr("rf_local_subtract(three, two)").as[ProjectedRasterTile].first(), one)

      val maybeOneTile =
        df.select(rf_local_subtract(ExtractTile($"three"), ExtractTile($"two"))).as[Tile]
      assertEqual(maybeOneTile.first(), one.toArrayTile())
      checkDocs("rf_local_subtract")
    }

    it("should rf_local_multiply") {
      val df = Seq((three, two)).toDF("three", "two")

      val maybeSix = df.select(rf_local_multiply($"three", $"two")).as[ProjectedRasterTile]
      assertEqual(maybeSix.first(), six)

      assertEqual(df.selectExpr("rf_local_multiply(three, two)").as[ProjectedRasterTile].first(), six)

      val maybeSixTile =
        df.select(rf_local_multiply(ExtractTile($"three"), ExtractTile($"two"))).as[Tile]
      assertEqual(maybeSixTile.first(), six.toArrayTile())
      checkDocs("rf_local_multiply")
    }

    it("should rf_local_divide") {
      val df = Seq((six, two)).toDF("six", "two")
      val maybeThree = df.select(rf_local_divide($"six", $"two")).as[ProjectedRasterTile]
      assertEqual(maybeThree.first(), three)

      assertEqual(df.selectExpr("rf_local_divide(six, two)").as[ProjectedRasterTile].first(), three)

      assertEqual(
        df.selectExpr("rf_local_multiply(rf_local_divide(six, 2.0), two)")
          .as[ProjectedRasterTile]
          .first(),
        six)

      val maybeThreeTile =
        df.select(rf_local_divide(ExtractTile($"six"), ExtractTile($"two"))).as[Tile]
      assertEqual(maybeThreeTile.first(), three.toArrayTile())
      checkDocs("rf_local_divide")
    }
  }

  describe("scalar tile operations") {
    it("should rf_local_add") {
      val df = Seq(one).toDF("one")
      val maybeThree = df.select(rf_local_add($"one", 2)).as[ProjectedRasterTile]
      assertEqual(maybeThree.first(), three)

      val maybeThreeD = df.select(rf_local_add($"one", 2.1)).as[ProjectedRasterTile]
      assertEqual(maybeThreeD.first(), three.convert(DoubleConstantNoDataCellType).localAdd(0.1))

      val maybeThreeTile = df.select(rf_local_add(ExtractTile($"one"), 2)).as[Tile]
      assertEqual(maybeThreeTile.first(), three.toArrayTile())
    }

    it("should rf_local_subtract") {
      val df = Seq(three).toDF("three")

      val maybeOne = df.select(rf_local_subtract($"three", 2)).as[ProjectedRasterTile]
      assertEqual(maybeOne.first(), one)

      val maybeOneD = df.select(rf_local_subtract($"three", 2.0)).as[ProjectedRasterTile]
      assertEqual(maybeOneD.first(), one)

      val maybeOneTile = df.select(rf_local_subtract(ExtractTile($"three"), 2)).as[Tile]
      assertEqual(maybeOneTile.first(), one.toArrayTile())
    }

    it("should rf_local_multiply") {
      val df = Seq(three).toDF("three")

      val maybeSix = df.select(rf_local_multiply($"three", 2)).as[ProjectedRasterTile]
      assertEqual(maybeSix.first(), six)

      val maybeSixD = df.select(rf_local_multiply($"three", 2.0)).as[ProjectedRasterTile]
      assertEqual(maybeSixD.first(), six)

      val maybeSixTile = df.select(rf_local_multiply(ExtractTile($"three"), 2)).as[Tile]
      assertEqual(maybeSixTile.first(), six.toArrayTile())
    }

    it("should rf_local_divide") {
      val df = Seq(six).toDF("six")

      val maybeThree = df.select(rf_local_divide($"six", 2)).as[ProjectedRasterTile]
      assertEqual(maybeThree.first(), three)

      val maybeThreeD = df.select(rf_local_divide($"six", 2.0)).as[ProjectedRasterTile]
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

    it("should render ascii art") {
      val df = Seq[Tile](ProjectedRasterTile(TestData.l8Labels)).toDF("tile")
      val r1 = df.select(rf_render_ascii($"tile"))
      val r2 = df.selectExpr("rf_render_ascii(tile)").as[String]
      r1.first() should be(r2.first())
      checkDocs("rf_render_ascii")
    }

    it("should render cells as matrix") {
      val df = Seq(randDoubleNDTile).toDF("tile")
      val r1 = df.select(rf_render_matrix($"tile"))
      val r2 = df.selectExpr("rf_render_matrix(tile)").as[String]
      r1.first() should be(r2.first())
      checkDocs("rf_render_matrix")
    }

    it("should round tile cell values") {

      val three_plus = TestData.projectedRasterTile(cols, rows, 3.12, extent, crs, DoubleConstantNoDataCellType)
      val three_less = TestData.projectedRasterTile(cols, rows, 2.92, extent, crs, DoubleConstantNoDataCellType)
      val three_double = TestData.projectedRasterTile(cols, rows, 3.0, extent, crs, DoubleConstantNoDataCellType)

      val df = Seq((three_plus, three_less, three)).toDF("three_plus", "three_less", "three")

      assertEqual(df.select(rf_round($"three")).as[ProjectedRasterTile].first(), three)
      assertEqual(df.select(rf_round($"three_plus")).as[ProjectedRasterTile].first(), three_double)
      assertEqual(df.select(rf_round($"three_less")).as[ProjectedRasterTile].first(), three_double)

      assertEqual(df.selectExpr("rf_round(three)").as[ProjectedRasterTile].first(), three)
      assertEqual(df.selectExpr("rf_round(three_plus)").as[ProjectedRasterTile].first(), three_double)
      assertEqual(df.selectExpr("rf_round(three_less)").as[ProjectedRasterTile].first(), three_double)

      checkDocs("rf_round")
    }

    it("should abs cell values") {
      val minus = one.mapTile(t => t.convert(IntConstantNoDataCellType) * -1)
      val df = Seq((minus, one)).toDF("minus", "one")

      assertEqual(df.select(rf_abs($"minus").as[ProjectedRasterTile]).first(), one)

      checkDocs("rf_abs")
    }

    it("should take logarithms positive cell values") {
      // rf_log10 1000 == 3
      val thousand = TestData.projectedRasterTile(cols, rows, 1000, extent, crs, ShortConstantNoDataCellType)
      val threesDouble = TestData.projectedRasterTile(cols, rows, 3.0, extent, crs, DoubleConstantNoDataCellType)
      val zerosDouble = TestData.projectedRasterTile(cols, rows, 0.0, extent, crs, DoubleConstantNoDataCellType)

      val df1 = Seq(thousand).toDF("tile")
      assertEqual(df1.select(rf_log10($"tile")).as[ProjectedRasterTile].first(), threesDouble)

      // ln random tile == rf_log10 random tile / rf_log10(e); random tile square to ensure all positive cell values
      val df2 = Seq(randPositiveDoubleTile).toDF("tile")
      val log10e = math.log10(math.E)
      assertEqual(
        df2.select(rf_log($"tile")).as[ProjectedRasterTile].first(),
        df2.select(rf_log10($"tile")).as[ProjectedRasterTile].first() / log10e)

      lazy val maybeZeros = df2
        .selectExpr(s"rf_local_subtract(rf_log(tile), rf_local_divide(rf_log10(tile), ${log10e}))")
        .as[ProjectedRasterTile]
        .first()
      assertEqual(maybeZeros, zerosDouble)

      // rf_log1p for zeros should be ln(1)
      val ln1 = math.log1p(0.0)
      val df3 = Seq(zero).toDF("tile")
      val maybeLn1 = df3.selectExpr(s"rf_log1p(tile)").as[ProjectedRasterTile].first()
      assert(maybeLn1.toArrayDouble().forall(_ == ln1))

      checkDocs("rf_log")
      checkDocs("rf_log2")
      checkDocs("rf_log10")
      checkDocs("rf_log1p")
    }

    it("should take logarithms with non-positive cell values") {
      val ni_float = TestData.projectedRasterTile(cols, rows, Double.NegativeInfinity, extent, crs, DoubleConstantNoDataCellType)
      val zero_float = TestData.projectedRasterTile(cols, rows, 0.0, extent, crs, DoubleConstantNoDataCellType)

      // tile zeros ==> -Infinity
      val df_0 = Seq(zero).toDF("tile")
      assertEqual(df_0.select(rf_log($"tile")).as[ProjectedRasterTile].first(), ni_float)
      assertEqual(df_0.select(rf_log10($"tile")).as[ProjectedRasterTile].first(), ni_float)
      assertEqual(df_0.select(rf_log2($"tile")).as[ProjectedRasterTile].first(), ni_float)
      // rf_log1p of zeros should be 0.
      assertEqual(df_0.select(rf_log1p($"tile")).as[ProjectedRasterTile].first(), zero_float)

      // tile negative values ==> NaN
      assert(df_0.selectExpr("rf_log(rf_local_subtract(tile, 42))").as[ProjectedRasterTile].first().isNoDataTile)
      assert(df_0.selectExpr("rf_log2(rf_local_subtract(tile, 42))").as[ProjectedRasterTile].first().isNoDataTile)
      assert(df_0.select(rf_log1p(rf_local_subtract($"tile", 42))).as[ProjectedRasterTile].first().isNoDataTile)
      assert(df_0.select(rf_log10(rf_local_subtract($"tile", lit(0.01)))).as[ProjectedRasterTile].first().isNoDataTile)

    }

    it("should take exponential") {
      val df = Seq(six).toDF("tile")

      // rf_exp inverses rf_log
      assertEqual(
        df.select(rf_exp(rf_log($"tile"))).as[ProjectedRasterTile].first(),
        six
      )

      // base 2
      assertEqual(df.select(rf_exp2(rf_log2($"tile"))).as[ProjectedRasterTile].first(), six)

      // base 10
      assertEqual(df.select(rf_exp10(rf_log10($"tile"))).as[ProjectedRasterTile].first(), six)

      // plus/minus 1
      assertEqual(df.select(rf_expm1(rf_log1p($"tile"))).as[ProjectedRasterTile].first(), six)

      // SQL
      assertEqual(df.selectExpr("rf_exp(rf_log(tile))").as[ProjectedRasterTile].first(), six)

      // SQL base 10
      assertEqual(df.selectExpr("rf_exp10(rf_log10(tile))").as[ProjectedRasterTile].first(), six)

      // SQL base 2
      assertEqual(df.selectExpr("rf_exp2(rf_log2(tile))").as[ProjectedRasterTile].first(), six)

      // SQL rf_expm1
      assertEqual(df.selectExpr("rf_expm1(rf_log1p(tile))").as[ProjectedRasterTile].first(), six)

      checkDocs("rf_exp")
      checkDocs("rf_exp10")
      checkDocs("rf_exp2")
      checkDocs("rf_expm1")

    }

    it("should take square root") {
      val df = Seq(three).toDF("tile")
      assertEqual(
        df.select(rf_sqrt(rf_local_multiply($"tile", $"tile"))).as[ProjectedRasterTile].first(),
        three
      )
    }

    it("should resample") {
      def lowRes = {
        def base = ArrayTile(Array(1, 2, 3, 4), 2, 2)

        ProjectedRasterTile(base.convert(ct), extent, crs)
      }

      def upsampled = {
        // format: off
        def base = ArrayTile(Array(
          1, 1, 2, 2,
          1, 1, 2, 2,
          3, 3, 4, 4,
          3, 3, 4, 4
        ), 4, 4)
        // format: on
        ProjectedRasterTile(base.convert(ct), extent, crs)
      }

      // a 4, 4 tile to upsample by shape
      def fourByFour = TestData.projectedRasterTile(4, 4, 0, extent, crs, ct)

      def df = Seq(lowRes).toDF("tile")

      val maybeUp = df.select(rf_resample($"tile", lit(2))).as[ProjectedRasterTile].first()
      assertEqual(maybeUp, upsampled)

      def df2 = Seq((lowRes, fourByFour)).toDF("tile1", "tile2")

      val maybeUpShape = df2.select(rf_resample($"tile1", $"tile2")).as[ProjectedRasterTile].first()
      assertEqual(maybeUpShape, upsampled)

      // Downsample by double argument < 1
      def df3 = Seq(upsampled).toDF("tile").withColumn("factor", lit(0.5))

      assertEqual(df3.selectExpr("rf_resample(tile, 0.5)").as[ProjectedRasterTile].first(), lowRes)
      assertEqual(df3.selectExpr("rf_resample(tile, factor)").as[ProjectedRasterTile].first(), lowRes)

      checkDocs("rf_resample")
    }

    it("should interpret cell values with a specified cell type") {
      checkDocs("rf_interpret_cell_type_as")
      val df = Seq(randNDPRT).toDF("t")
        .withColumn("tile", rf_interpret_cell_type_as($"t", "int8raw"))
      val resultTile = df.select("tile").as[Tile].first()

      resultTile.cellType should be(CellType.fromName("int8raw"))
      // should have same number of values that are -2 the old ND
      val countOldNd = df.select(
        rf_tile_sum(rf_local_equal($"tile", ct.noDataValue)),
        rf_no_data_cells($"t")
      ).first()
      countOldNd._1 should be(countOldNd._2)

      // should not have no data any more (raw type)
      val countNewNd = df.select(rf_no_data_cells($"tile")).first()
      countNewNd should be(0L)
    }

    it("should check values is_in") {
      checkDocs("rf_local_is_in")

      // tile is 3 by 3 with values, 1 to 9
      val rf = Seq(byteArrayTile).toDF("t")
        .withColumn("one", lit(1))
        .withColumn("five", lit(5))
        .withColumn("ten", lit(10))
        .withColumn("in_expect_2", rf_local_is_in($"t", array($"one", $"five")))
        .withColumn("in_expect_1", rf_local_is_in($"t", array($"ten", $"five")))
        .withColumn("in_expect_1a", rf_local_is_in($"t", Array(10, 5)))
        .withColumn("in_expect_0", rf_local_is_in($"t", array($"ten")))

      val e2Result = rf.select(rf_tile_sum($"in_expect_2")).as[Double].first()
      e2Result should be(2.0)

      val e1Result = rf.select(rf_tile_sum($"in_expect_1")).as[Double].first()
      e1Result should be(1.0)

      val e1aResult = rf.select(rf_tile_sum($"in_expect_1a")).as[Double].first()
      e1aResult should be(1.0)

      val e0Result = rf.select($"in_expect_0").as[Tile].first()
      e0Result.toArray() should contain only (0)
    }
  }
}
