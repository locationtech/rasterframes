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

import geotrellis.raster
import geotrellis.raster._
import geotrellis.raster.testkit.RasterMatchers
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._
import org.locationtech.rasterframes.expressions.accessors.ExtractTile
import org.locationtech.rasterframes.stats._
import org.locationtech.rasterframes.tiles.ProjectedRasterTile


import geotrellis.raster.testkit.RasterMatchers

class MaskingFunctionsSpec extends TestEnvironment with RasterMatchers {
  import TestData._
  import spark.implicits._
  import ProjectedRasterTile.prtEncoder

  implicit val pairEnc = Encoders.tuple(ProjectedRasterTile.prtEncoder, ProjectedRasterTile.prtEncoder)
  implicit val tripEnc = Encoders.tuple(ProjectedRasterTile.prtEncoder, ProjectedRasterTile.prtEncoder, ProjectedRasterTile.prtEncoder)


  describe("masking by defined") {
    it("should mask one tile against another") {
      val df = Seq[Tile](randPRT).toDF("tile")

      val withMask = df.withColumn("mask",
        rf_convert_cell_type(
          rf_local_greater($"tile", 50),
          "uint8")
      )

      val withMasked = withMask.withColumn("masked",
        rf_mask($"tile", $"mask"))

      val result = withMasked.agg(rf_agg_no_data_cells($"tile") < rf_agg_no_data_cells($"masked")).as[Boolean]

      result.first() should be(true)

      checkDocs("rf_mask")
    }

    it("should mask with expected results") {
      val df = Seq((byteArrayTile, maskingTile)).toDF("tile", "mask")

      val withMasked = df.withColumn("masked",
        rf_mask($"tile", $"mask"))

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

      val withMask = df.withColumn("mask",
        rf_convert_cell_type(
          rf_local_greater($"tile", 50),
          "uint8"
        )
      )

      val withMasked = withMask
        .withColumn("masked", rf_mask($"tile", $"mask"))
        .withColumn("inv_masked", rf_inverse_mask($"tile", $"mask"))

      val result = withMasked.agg(rf_agg_no_data_cells($"masked") + rf_agg_no_data_cells($"inv_masked")).as[Long]

      result.first() should be(tileSize + baseND)

      checkDocs("rf_inverse_mask")
    }

    it("should throw if no nodata"){
      val noNoDataCellType = UByteCellType

      val df = Seq(TestData.projectedRasterTile(5, 5, 42, TestData.extent, TestData.crs,
        noNoDataCellType))
        .toDF("tile")
      // if this had NoData defined would be a no-op because the tile is all datacells
      lazy val result = df.select(rf_mask($"tile", $"tile")).collect()
      an[AssertionError] should be thrownBy(result)
    }
  }

  describe("mask by value") {

    it("should mask tile by another identified by specified value") {
      val df = Seq[Tile](randPRT).toDF("tile")
      val mask_value = 4

      val withMask = df.withColumn("mask",
        rf_local_multiply(rf_convert_cell_type(
          rf_local_greater($"tile", 50),
          "uint8"),
          lit(mask_value)
        )
      )

      val withMasked = withMask.withColumn("masked",
        rf_mask_by_value($"tile", $"mask", lit(mask_value)))

      val result = withMasked.agg(rf_agg_no_data_cells($"tile") < rf_agg_no_data_cells($"masked")).as[Boolean]

      result.first() should be(true)
      checkDocs("rf_mask_by_value")
    }

    it("should mask by value for value 0.") {
      // maskingTile has -4, ND, and -15 values. Expect mask by value with 0 to not change the
      val df = Seq((byteArrayTile, maskingTile)).toDF("data", "mask")

      // data tile is all data cells
      df.select(rf_data_cells($"data")).first() should be (byteArrayTile.size)

      // mask by value against 15 should set 3 cell locations to Nodata
      df.withColumn("mbv", rf_mask_by_value($"data", $"mask", 15))
        .select(rf_data_cells($"mbv"))
        .first() should be (byteArrayTile.size - 3)

      // breaks with issue https://github.com/locationtech/rasterframes/issues/416
      val result = df.withColumn("mbv", rf_mask_by_value($"data", $"mask", 0))
        .select(rf_data_cells($"mbv"))
        .first()
      result should be (byteArrayTile.size)
    }

    it("should inverse mask tile by another identified by specified value") {
      val df = Seq[Tile](randPRT).toDF("tile")
      val mask_value = 4

      val withMask = df.withColumn("mask",
        rf_local_multiply(rf_convert_cell_type(
          rf_local_greater($"tile", 50),
          "uint8"),
          mask_value
        )
      )

      val withMasked = withMask.withColumn("masked",
        rf_inverse_mask_by_value($"tile", $"mask", mask_value))
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

      val withMasked = df.withColumn("masked",
        rf_mask_by_values($"tile", $"mask", mask_values:_*))

      val expected = squareIncrementingPRT.toArray().count(v ⇒ mask_values.contains(v))

      val result = withMasked.agg(rf_agg_no_data_cells($"masked") as "masked_nd")
        .first()

      result.getAs[BigInt](0) should be(expected)

      val withMaskedSql = df.selectExpr("rf_mask_by_values(tile, mask, array(4, 5, 6, 12)) AS masked")
      val resultSql = withMaskedSql.agg(rf_agg_no_data_cells($"masked")).as[Long]
      resultSql.first() should be(expected)

      checkDocs("rf_mask_by_values")
    }
  }

  describe("mask by bit extraction"){

      // Define a dataframe set up similar to the Landsat8 masking scheme
      // Sample of https://www.usgs.gov/media/images/landsat-8-quality-assessment-band-pixel-value-interpretations
      val fill = 1
      val clear = 2720
      val cirrus = 6816
      val med_cloud = 2756 // with 1-2 bands saturated
      val hi_cirrus = 6900 // yes cloud, hi conf cloud and hi conf cirrus and 1-2band sat
      val dataColumnCellType = UShortConstantNoDataCellType
      val tiles = Seq(fill, clear, cirrus, med_cloud, hi_cirrus).map{v ⇒
        (
          TestData.projectedRasterTile(3, 3, 6, TestData.extent, TestData.crs, dataColumnCellType),
          TestData.projectedRasterTile(3, 3, v, TestData.extent, TestData.crs, UShortCellType) // because masking returns the union of cell types
        )
      }

      val df = tiles.toDF("data", "mask")
        .withColumn("val", rf_tile_min($"mask"))

    it("should give LHS cell type"){
        val resultMask = df.select(
          rf_cell_type(
            rf_mask($"data", $"mask")
          )
        ).distinct().collect()
        all (resultMask) should be (dataColumnCellType)

        val resultMaskVal = df.select(
          rf_cell_type(
            rf_mask_by_value($"data", $"mask", 5)
          )
        ).distinct().collect()

        all(resultMaskVal) should be (dataColumnCellType)

        val resultMaskValues = df.select(
          rf_cell_type(
            rf_mask_by_values($"data", $"mask", 5, 6, 7 )
          )
        ).distinct().collect()
        all(resultMaskValues) should be (dataColumnCellType)

        val resultMaskBit = df.select(
          rf_cell_type(
            rf_mask_by_bit($"data", $"mask", 5, true)
          )
        ).distinct().collect()
        all(resultMaskBit) should be (dataColumnCellType)

        val resultMaskValInv = df.select(
          rf_cell_type(
            rf_inverse_mask_by_value($"data", $"mask", 5)
          )
        ).distinct().collect()
        all(resultMaskValInv) should be (dataColumnCellType)

      }


    it("should unpack QA bits"){
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
        result.filter($"val" === lit(valFilter))
          .select(col(colName))
          .as[ProjectedRasterTile]
          .first()
          .get(0, 0) should be (assertValue)
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
        .select(rf_exists($"t")).as[Boolean].first() should be (false)

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
          all(extracted.toArray()) should be (expectedValue)
        }
      }

      Seq(
        TestCase(BitCellType, 1, 0, 1, 1),
        TestCase(ByteCellType, 127, 6, 2, 1), // 7th bit is sign
        TestCase(ByteCellType, 127, 5, 2, 3),
        TestCase(ByteCellType, -128, 6, 2, 2), // 7th bit is sign
        TestCase(UByteCellType, 255, 6, 2, 3),
        TestCase(UByteCellType, 255, 10, 2, 0),  // shifting beyond range of cell type results in 0
        TestCase(ShortCellType, 32767, 15, 1, 0),
        TestCase(ShortCellType, 32767, 14, 2, 1),
        TestCase(ShortUserDefinedNoDataCellType(0), -32768, 14, 2, 2),
        TestCase(UShortCellType, 65535, 14, 2, 3),
        TestCase(UShortCellType, 65535, 18, 2, 0),  // shifting beyond range of cell type results in 0
        TestCase(IntCellType, 2147483647, 30, 2, 1),
        TestCase(IntCellType, 2147483647, 29, 2, 3)
      ).foreach(_.testIt)

      // floating point types
      an [AssertionError] should be thrownBy TestCase[Float](FloatCellType, Float.MaxValue, 29, 2, 3).testIt()

    }

    it("should mask by QA bits"){
      val result = df
        .withColumn("fill_no", rf_mask_by_bit($"data", $"mask", 0, true))
        .withColumn("sat_0", rf_mask_by_bits($"data", $"mask", 2, 2, 1, 2, 3)) // strict no bands
        .withColumn("sat_2", rf_mask_by_bits($"data", $"mask", 2, 2, 2, 3)) // up to 2 bands contain sat
        .withColumn("sat_4",
          rf_mask_by_bits($"data", $"mask", lit(2), lit(2), array(lit(3)))) // up to 4 bands contain sat
        .withColumn("cloud_no", rf_mask_by_bit($"data", $"mask", lit(4), lit(true)))
        .withColumn("cloud_only", rf_mask_by_bit($"data", $"mask", 4, false)) // mask if *not* cloud
        .withColumn("cloud_conf_low", rf_mask_by_bits($"data", $"mask", lit(5), lit(2), array(lit(0), lit(1))))
        .withColumn("cloud_conf_med", rf_mask_by_bits($"data", $"mask", 5, 2, 0, 1, 2))
        .withColumn("cirrus_med", rf_mask_by_bits($"data", $"mask", 11, 2, 3, 2)) // n.b. this is masking out more likely cirrus.

      result.select(rf_cell_type($"fill_no")).first() should be (dataColumnCellType)

      def checker(columnName: String, maskValueFilter: Int, resultIsNoData: Boolean = true): Unit = {
        /**  in this unit test setup, the `val` column is an integer that the entire row's mask is full of
          * filter for the maskValueFilter
          * then check the columnName and look at the masked data tile given by `columnName`
          * assert that the `columnName` tile is / is not all nodata based on `resultIsNoData`
          * */

        val printOutcome = if (resultIsNoData) "all NoData cells"
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

        resultToCheck should be (resultIsNoData)
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

    it("should have SQL equivalent to mask bits"){

      df.createOrReplaceTempView("df_maskbits")

      val maskedCol = "cloud_conf_med"
      // this is the example in the docs
      val result = spark.sql(
        s"""
           |SELECT rf_mask_by_values(
           |            data,
           |            rf_local_extract_bits(mask, 5, 2),
           |            array(0, 1, 2)
           |            ) as ${maskedCol}
           | FROM df_maskbits
           | WHERE val = 2756
           |""".stripMargin)
      result.select(rf_is_no_data_tile(col(maskedCol))).first() should be (true)

    }
  }

}
