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
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._
import org.locationtech.rasterframes.expressions.accessors.ExtractTile
import org.locationtech.rasterframes.tiles.ProjectedRasterTile

class RasterFunctionsSpec extends TestEnvironment with RasterMatchers {
  import TestData._
  import spark.implicits._

  implicit val pairEnc = Encoders.tuple(ProjectedRasterTile.prtEncoder, ProjectedRasterTile.prtEncoder)
  implicit val tripEnc = Encoders.tuple(ProjectedRasterTile.prtEncoder, ProjectedRasterTile.prtEncoder, ProjectedRasterTile.prtEncoder)

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

    it("should mask one tile against another") {
      val df = Seq[Tile](randPRT).toDF("tile")

      val withMask = df.withColumn("mask", rf_convert_cell_type(rf_local_greater($"tile", 50), "uint8"))

      val withMasked = withMask.withColumn("masked", rf_mask($"tile", $"mask"))

      val result = withMasked.agg(rf_agg_no_data_cells($"tile") < rf_agg_no_data_cells($"masked")).as[Boolean]

      result.first() should be(true)

      checkDocs("rf_mask")
    }

    it("should mask with expected results") {
      val df = Seq((byteArrayTile, maskingTile)).toDF("tile", "mask")

      val withMasked = df.withColumn("masked", rf_mask($"tile", $"mask"))

      val result: Tile = withMasked.select($"masked").as[Tile].first()

      result.localUndefined().toArray() should be(maskingTile.localUndefined().toArray())
    }

    it("should mask without mutating cell type") {
      val result = Seq((byteArrayTile, maskingTile))
        .toDF("tile", "mask")
        .select(rf_mask($"tile", $"mask").as("masked_tile"))
        .select(rf_cell_type($"masked_tile"))
        .first()

      result should be(byteArrayTile.cellType)
    }

    it("should inverse mask one tile against another") {
      val df = Seq[Tile](randPRT).toDF("tile")

      val baseND = df.select(rf_agg_no_data_cells($"tile")).first()

      val withMask = df.withColumn(
        "mask",
        rf_convert_cell_type(
          rf_local_greater($"tile", 50),
          "uint8"
        ))

      val withMasked = withMask
        .withColumn("masked", rf_mask($"tile", $"mask"))
        .withColumn("inv_masked", rf_inverse_mask($"tile", $"mask"))

      val result = withMasked.agg(rf_agg_no_data_cells($"masked") + rf_agg_no_data_cells($"inv_masked")).as[Long]

      result.first() should be(tileSize + baseND)

      checkDocs("rf_inverse_mask")
    }

    it("should mask tile by another identified by specified value") {
      val df = Seq[Tile](randPRT).toDF("tile")
      val mask_value = 4

      val withMask =
        df.withColumn("mask", rf_local_multiply(rf_convert_cell_type(rf_local_greater($"tile", 50), "uint8"), lit(mask_value)))

      val withMasked = withMask.withColumn("masked", rf_mask_by_value($"tile", $"mask", lit(mask_value)))

      val result = withMasked.agg(rf_agg_no_data_cells($"tile") < rf_agg_no_data_cells($"masked")).as[Boolean]

      result.first() should be(true)
      checkDocs("rf_mask_by_value")
    }

    it("should mask by value for value 0.") {
      // maskingTile has -4, ND, and -15 values. Expect mask by value with 0 to not change the
      val df = Seq((byteArrayTile, maskingTile)).toDF("data", "mask")

      // data tile is all data cells
      df.select(rf_data_cells($"data")).first() should be(byteArrayTile.size)

      // mask by value against 15 should set 3 cell locations to Nodata
      df.withColumn("mbv", rf_mask_by_value($"data", $"mask", 15))
        .select(rf_data_cells($"mbv"))
        .first() should be(byteArrayTile.size - 3)

      // breaks with issue https://github.com/locationtech/rasterframes/issues/416
      val result = df
        .withColumn("mbv", rf_mask_by_value($"data", $"mask", 0))
        .select(rf_data_cells($"mbv"))
        .first()
      result should be(byteArrayTile.size)
    }

    it("should inverse mask tile by another identified by specified value") {
      val df = Seq[Tile](randPRT).toDF("tile")
      val mask_value = 4

      val withMask = df.withColumn("mask", rf_local_multiply(rf_convert_cell_type(rf_local_greater($"tile", 50), "uint8"), mask_value))

      val withMasked = withMask
        .withColumn("masked", rf_inverse_mask_by_value($"tile", $"mask", mask_value))
        .withColumn("masked2", rf_mask_by_value($"tile", $"mask", lit(mask_value), true))

      val result = withMasked.agg(rf_agg_no_data_cells($"tile") < rf_agg_no_data_cells($"masked")).as[Boolean]

      result.first() should be(true)

      val result2 = withMasked.agg(rf_agg_no_data_cells($"tile") < rf_agg_no_data_cells($"masked2")).as[Boolean]
      result2.first() should be(true)

      checkDocs("rf_inverse_mask_by_value")
    }

    it("should mask tile by another identified by sequence of specified values") {
      val squareIncrementingPRT = ProjectedRasterTile(squareIncrementingTile(six.rows), six.extent, six.crs)
      val df = Seq((six, squareIncrementingPRT))
        .toDF("tile", "mask")

      val mask_values = Seq(4, 5, 6, 12)

      val withMasked = df.withColumn("masked", rf_mask_by_values($"tile", $"mask", mask_values: _*))

      val expected = squareIncrementingPRT.toArray().count(v => mask_values.contains(v))

      val result = withMasked
        .agg(rf_agg_no_data_cells($"masked") as "masked_nd")
        .first()

      result.getAs[BigInt](0) should be(expected)

      val withMaskedSql = df.selectExpr("rf_mask_by_values(tile, mask, array(4, 5, 6, 12)) AS masked")
      val resultSql = withMaskedSql.agg(rf_agg_no_data_cells($"masked")).as[Long]
      resultSql.first() should be(expected)

      checkDocs("rf_mask_by_values")
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
  }

  it("should resample") {
    def lowRes = {
      def base = ArrayTile(Array(1, 2, 3, 4), 2, 2)
      ProjectedRasterTile(base.convert(ct), extent, crs)
    }
    def upsampled = {
      // format: off
      def base = ArrayTile(Array(
        1,1,2,2,
        1,1,2,2,
        3,3,4,4,
        3,3,4,4
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

  describe("masking by specific bit values") {
    // Define a dataframe set up similar to the Landsat8 masking scheme
    // Sample of https://www.usgs.gov/media/images/landsat-8-quality-assessment-band-pixel-value-interpretations
    val fill = 1
    val clear = 2720
    val cirrus = 6816
    val med_cloud = 2756 // with 1-2 bands saturated
    val hi_cirrus = 6900 // yes cloud, hi conf cloud and hi conf cirrus and 1-2band sat
    val dataColumnCellType = UShortConstantNoDataCellType
    val tiles = Seq(fill, clear, cirrus, med_cloud, hi_cirrus).map { v =>
      (
        TestData.projectedRasterTile(3, 3, 6, TestData.extent, TestData.crs, dataColumnCellType),
        TestData
          .projectedRasterTile(3, 3, v, TestData.extent, TestData.crs, UShortCellType) // because masking returns the union of cell types
      )
    }

    val df = tiles
      .toDF("data", "mask")
      .withColumn("val", rf_tile_min($"mask"))

    it("should give LHS cell type") {
      val resultMask = df
        .select(
          rf_cell_type(
            rf_mask($"data", $"mask")
          )
        )
        .distinct()
        .collect()
      all(resultMask) should be(dataColumnCellType)

      val resultMaskVal = df
        .select(
          rf_cell_type(
            rf_mask_by_value($"data", $"mask", 5)
          )
        )
        .distinct()
        .collect()

      all(resultMaskVal) should be(dataColumnCellType)

      val resultMaskValues = df
        .select(
          rf_cell_type(
            rf_mask_by_values($"data", $"mask", 5, 6, 7)
          )
        )
        .distinct()
        .collect()
      all(resultMaskValues) should be(dataColumnCellType)

      val resultMaskBit = df
        .select(
          rf_cell_type(
            rf_mask_by_bit($"data", $"mask", 5, true)
          )
        )
        .distinct()
        .collect()
      all(resultMaskBit) should be(dataColumnCellType)

      val resultMaskValInv = df
        .select(
          rf_cell_type(
            rf_inverse_mask_by_value($"data", $"mask", 5)
          )
        )
        .distinct()
        .collect()
      all(resultMaskValInv) should be(dataColumnCellType)

    }

    it("should check values isin") {
      checkDocs("rf_local_is_in")

      // tile is 3 by 3 with values, 1 to 9
      val rf = Seq(byteArrayTile)
        .toDF("t")
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
    it("should unpack QA bits") {
      checkDocs("rf_local_extract_bits")

      val result = df
        .withColumn("qa_fill", rf_local_extract_bits($"mask", lit(0)))
        .withColumn("qa_sat", rf_local_extract_bits($"mask", lit(2), lit(2)))
        .withColumn("qa_cloud", rf_local_extract_bits($"mask", lit(4)))
        .withColumn("qa_cconf", rf_local_extract_bits($"mask", 5, 2))
        .withColumn("qa_snow", rf_local_extract_bits($"mask", lit(9), lit(2)))
        .withColumn("qa_circonf", rf_local_extract_bits($"mask", 11, 2))

      def checker(colName: String, valFilter: Int, assertValue: Int): Unit = {
        // print this so we can see what's happening if something  wrong
        println(s"${colName} should be ${assertValue} for qa val ${valFilter}")
        result
          .filter($"val" === lit(valFilter))
          .select(col(colName))
          .as[ProjectedRasterTile]
          .first()
          .get(0, 0) should be(assertValue)
      }

      checker("qa_fill", fill, 1)
      checker("qa_cloud", fill, 0)
      checker("qa_cconf", fill, 0)
      checker("qa_sat", fill, 0)
      checker("qa_snow", fill, 0)
      checker("qa_circonf", fill, 0)

      // trivial bits selection (numBits=0) and SQL
      df.filter($"val" === lit(fill))
        .selectExpr("rf_local_extract_bits(mask, 0, 0) AS t")
        .select(rf_exists($"t"))
        .as[Boolean]
        .first() should be(false)

      checker("qa_fill", clear, 0)
      checker("qa_cloud", clear, 0)
      checker("qa_cconf", clear, 1)

      checker("qa_fill", med_cloud, 0)
      checker("qa_cloud", med_cloud, 0)
      checker("qa_cconf", med_cloud, 2) // L8 only tags hi conf in the cloud assessment
      checker("qa_sat", med_cloud, 1)

      checker("qa_fill", cirrus, 0)
      checker("qa_sat", cirrus, 0)
      checker("qa_cloud", cirrus, 0) //low cloud conf
      checker("qa_cconf", cirrus, 1) //low cloud conf
      checker("qa_circonf", cirrus, 3) //high cirrus  conf
    }
    it("should extract bits from different cell types") {
      import org.locationtech.rasterframes.expressions.transformers.ExtractBits

      case class TestCase[N: Numeric](cellType: CellType, cellValue: N, bitPosition: Int, numBits: Int, expectedValue: Int) {
        def testIt(): Unit = {
          val tile = projectedRasterTile(3, 3, cellValue, TestData.extent, TestData.crs, cellType)
          val extracted = ExtractBits(tile, bitPosition, numBits)
          all(extracted.toArray()) should be(expectedValue)
        }
      }

      Seq(
        TestCase(BitCellType, 1, 0, 1, 1),
        TestCase(ByteCellType, 127, 6, 2, 1), // 7th bit is sign
        TestCase(ByteCellType, 127, 5, 2, 3),
        TestCase(ByteCellType, -128, 6, 2, 2), // 7th bit is sign
        TestCase(UByteCellType, 255, 6, 2, 3),
        TestCase(UByteCellType, 255, 10, 2, 0), // shifting beyond range of cell type results in 0
        TestCase(ShortCellType, 32767, 15, 1, 0),
        TestCase(ShortCellType, 32767, 14, 2, 1),
        TestCase(ShortUserDefinedNoDataCellType(0), -32768, 14, 2, 2),
        TestCase(UShortCellType, 65535, 14, 2, 3),
        TestCase(UShortCellType, 65535, 18, 2, 0), // shifting beyond range of cell type results in 0
        TestCase(IntCellType, 2147483647, 30, 2, 1),
        TestCase(IntCellType, 2147483647, 29, 2, 3)
      ).foreach(_.testIt)

      // floating point types
      an[AssertionError] should be thrownBy TestCase[Float](FloatCellType, Float.MaxValue, 29, 2, 3).testIt()

    }
    it("should mask by QA bits") {
      val result = df
        .withColumn("fill_no", rf_mask_by_bit($"data", $"mask", 0, true))
        .withColumn("sat_0", rf_mask_by_bits($"data", $"mask", 2, 2, 1, 2, 3)) // strict no bands
        .withColumn("sat_2", rf_mask_by_bits($"data", $"mask", 2, 2, 2, 3)) // up to 2 bands contain sat
        .withColumn("sat_4", rf_mask_by_bits($"data", $"mask", lit(2), lit(2), array(lit(3)))) // up to 4 bands contain sat
        .withColumn("cloud_no", rf_mask_by_bit($"data", $"mask", lit(4), lit(true)))
        .withColumn("cloud_only", rf_mask_by_bit($"data", $"mask", 4, false)) // mask if *not* cloud
        .withColumn("cloud_conf_low", rf_mask_by_bits($"data", $"mask", lit(5), lit(2), array(lit(0), lit(1))))
        .withColumn("cloud_conf_med", rf_mask_by_bits($"data", $"mask", 5, 2, 0, 1, 2))
        .withColumn("cirrus_med", rf_mask_by_bits($"data", $"mask", 11, 2, 3, 2)) // n.b. this is masking out more likely cirrus.

      result.select(rf_cell_type($"fill_no")).first() should be(dataColumnCellType)

      def checker(columnName: String, maskValueFilter: Int, resultIsNoData: Boolean = true): Unit = {

        /**  in this unit test setup, the `val` column is an integer that the entire row's mask is full of
         * filter for the maskValueFilter
         * then check the columnName and look at the masked data tile given by `columnName`
         * assert that the `columnName` tile is / is not all nodata based on `resultIsNoData`
         * */
        val printOutcome =
          if (resultIsNoData) "all NoData cells"
          else "all data cells"

        println(s"${columnName} should contain ${printOutcome} for qa val ${maskValueFilter}")
        val resultDf = result
          .filter($"val" === lit(maskValueFilter))

        val resultToCheck: Boolean = resultDf
          .select(rf_is_no_data_tile(col(columnName)))
          .first()

        val dataTile = resultDf.select(col(columnName)).as[ProjectedRasterTile].first()
        println(s"\tData tile values for col ${columnName}: ${dataTile.toArray().mkString(",")}")
//        val celltype = resultDf.select(rf_cell_type(col(columnName))).as[CellType].first()
//        println(s"Cell type for col ${columnName}: ${celltype}")

        resultToCheck should be(resultIsNoData)
      }

      checker("fill_no", fill, true)
      checker("cloud_only", clear, true)
      checker("cloud_only", hi_cirrus, false)
      checker("cloud_no", hi_cirrus, false)
      checker("sat_0", clear, false)
      checker("cloud_no", clear, false)
      checker("cloud_no", med_cloud, false)
      checker("cloud_conf_low", med_cloud, false)
      checker("cloud_conf_med", med_cloud, true)
      checker("cirrus_med", cirrus, true)
      checker("cloud_no", cirrus, false)
    }

    it("mask bits should have SQL equivalent") {

      df.createOrReplaceTempView("df_maskbits")

      val maskedCol = "cloud_conf_med"
      // this is the example in the docs
      val result = spark.sql(s"""
          |SELECT rf_mask_by_values(
          |            data,
          |            rf_local_extract_bits(mask, 5, 2),
          |            array(0, 1, 2)
          |            ) as ${maskedCol}
          | FROM df_maskbits
          | WHERE val = 2756
          |""".stripMargin)
      result.select(rf_is_no_data_tile(col(maskedCol))).first() should be(true)

    }
  }

}
