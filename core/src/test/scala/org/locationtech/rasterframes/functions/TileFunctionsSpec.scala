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

package org.locationtech.rasterframes.functions
import java.io.ByteArrayInputStream

import geotrellis.proj4.CRS
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.raster._
import javax.imageio.ImageIO
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions.sum
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.model.TileDimensions
import org.locationtech.rasterframes.stats._
import org.locationtech.rasterframes.tiles.ProjectedRasterTile
import org.locationtech.rasterframes.util.ColorRampNames

class TileFunctionsSpec extends TestEnvironment with RasterMatchers {
  import TestData._
  import spark.implicits._


  describe("constant tile generation operations") {
    val dim = 2
    val rows = 2

    it("should create a ones tile") {
      val df = (0 until rows)
        .toDF("id")
        .withColumn("const", rf_make_ones_tile(dim, dim, IntConstantNoDataCellType))
      val result = df.select(rf_tile_sum($"const") as "ts").agg(sum("ts")).as[Double].first()
      result should be(dim * dim * rows)
    }

    it("should create a zeros tile") {
      val df = (0 until rows)
        .toDF("id")
        .withColumn("const", rf_make_zeros_tile(dim, dim, FloatConstantNoDataCellType))
      val result = df.select(rf_tile_sum($"const") as "ts").agg(sum("ts")).as[Double].first()
      result should be(0)
    }

    it("should create an arbitrary constant tile") {
      val value = 4
      val df = (0 until rows)
        .toDF("id")
        .withColumn("const", rf_make_constant_tile(value, dim, dim, ByteConstantNoDataCellType))
      val result = df.select(rf_tile_sum($"const") as "ts").agg(sum("ts")).as[Double].first()
      result should be(dim * dim * rows * value)
    }
  }

  describe("cell type operations") {
    it("should convert cell type") {
      val df = Seq((TestData.injectND(7)(three), TestData.injectND(12)(two))).toDF("three", "two")

      val ct = df.select(
        rf_convert_cell_type($"three", "uint16ud512") as "three",
        rf_convert_cell_type($"two", "float32") as "two"
      )

      val (ct3, ct2) = ct.as[(Tile, Tile)].first()

      ct3.cellType should be(UShortUserDefinedNoDataCellType(512))
      ct2.cellType should be(FloatConstantNoDataCellType)

      val (cnt3, cnt2) = ct.select(rf_no_data_cells($"three"), rf_no_data_cells($"two")).as[(Long, Long)].first()

      cnt3 should be(7)
      cnt2 should be(12)

      checkDocs("rf_convert_cell_type")
    }
    it("should change NoData value") {
      val df = Seq((TestData.injectND(7)(three), TestData.injectND(12)(two))).toDF("three", "two")

      val ndCT = df.select(
        rf_with_no_data($"three", 3) as "three",
        rf_with_no_data($"two", 2.0) as "two"
      )

      val (cnt3, cnt2) = ndCT.select(rf_no_data_cells($"three"), rf_no_data_cells($"two")).as[(Long, Long)].first()

      cnt3 should be((cols * rows) - 7)
      cnt2 should be((cols * rows) - 12)

      checkDocs("rf_with_no_data")

      // Should maintain original cell type.
      ndCT.select(rf_cell_type($"two")).first().withDefaultNoData() should be(ct.withDefaultNoData())
    }
    it("should interpret cell values with a specified cell type") {
      checkDocs("rf_interpret_cell_type_as")
      val df = Seq(randNDPRT)
        .toDF("t")
        .withColumn("tile", rf_interpret_cell_type_as($"t", "int8raw"))
      val resultTile = df.select("tile").as[Tile].first()

      resultTile.cellType should be(CellType.fromName("int8raw"))
      // should have same number of values that are -2 the old ND
      val countOldNd = df
        .select(
          rf_tile_sum(rf_local_equal($"tile", ct.noDataValue)),
          rf_no_data_cells($"t")
        )
        .first()
      countOldNd._1 should be(countOldNd._2)

      // should not have no data any more (raw type)
      val countNewNd = df.select(rf_no_data_cells($"tile")).first()
      countNewNd should be(0L)
    }
  }

  describe("tile comparison relations") {
    it("should evaluate rf_local_less") {
      val df = Seq((two, three, six)).toDF("two", "three", "six")
      df.select(rf_tile_sum(rf_local_less($"two", 6))).first() should be(100.0)
      df.select(rf_tile_sum(rf_local_less($"two", 1.9))).first() should be(0.0)
      df.select(rf_tile_sum(rf_local_less($"two", 2))).first() should be(0.0)
      df.select(rf_tile_sum(rf_local_less($"three", $"two"))).first() should be(0.0)
      df.select(rf_tile_sum(rf_local_less($"three", $"three"))).first() should be(0.0)
      df.select(rf_tile_sum(rf_local_less($"three", $"six"))).first() should be(100.0)

      df.selectExpr("rf_tile_sum(rf_local_less(two, 6))").as[Double].first() should be(100.0)
      df.selectExpr("rf_tile_sum(rf_local_less(three, three))").as[Double].first() should be(0.0)
      checkDocs("rf_local_less")
    }

    it("should evaluate rf_local_less_equal") {
      val df = Seq((two, three, six)).toDF("two", "three", "six")
      df.select(rf_tile_sum(rf_local_less_equal($"two", 6))).first() should be(100.0)
      df.select(rf_tile_sum(rf_local_less_equal($"two", 1.9))).first() should be(0.0)
      df.select(rf_tile_sum(rf_local_less_equal($"two", 2))).first() should be(100.0)
      df.select(rf_tile_sum(rf_local_less_equal($"three", $"two"))).first() should be(0.0)
      df.select(rf_tile_sum(rf_local_less_equal($"three", $"three"))).first() should be(100.0)
      df.select(rf_tile_sum(rf_local_less_equal($"three", $"six"))).first() should be(100.0)

      df.selectExpr("rf_tile_sum(rf_local_less_equal(two, 6))").as[Double].first() should be(100.0)
      df.selectExpr("rf_tile_sum(rf_local_less_equal(three, three))").as[Double].first() should be(100.0)
      checkDocs("rf_local_less_equal")
    }

    it("should evaluate rf_local_greater") {
      val df = Seq((two, three, six)).toDF("two", "three", "six")
      df.select(rf_tile_sum(rf_local_greater($"two", 6))).first() should be(0.0)
      df.select(rf_tile_sum(rf_local_greater($"two", 1.9))).first() should be(100.0)
      df.select(rf_tile_sum(rf_local_greater($"two", 2))).first() should be(0.0)
      df.select(rf_tile_sum(rf_local_greater($"three", $"two"))).first() should be(100.0)
      df.select(rf_tile_sum(rf_local_greater($"three", $"three"))).first() should be(0.0)
      df.select(rf_tile_sum(rf_local_greater($"three", $"six"))).first() should be(0.0)

      df.selectExpr("rf_tile_sum(rf_local_greater(two, 1.9))").as[Double].first() should be(100.0)
      df.selectExpr("rf_tile_sum(rf_local_greater(three, three))").as[Double].first() should be(0.0)
      checkDocs("rf_local_greater")
    }

    it("should evaluate rf_local_greater_equal") {
      val df = Seq((two, three, six)).toDF("two", "three", "six")
      df.select(rf_tile_sum(rf_local_greater_equal($"two", 6))).first() should be(0.0)
      df.select(rf_tile_sum(rf_local_greater_equal($"two", 1.9))).first() should be(100.0)
      df.select(rf_tile_sum(rf_local_greater_equal($"two", 2))).first() should be(100.0)
      df.select(rf_tile_sum(rf_local_greater_equal($"three", $"two"))).first() should be(100.0)
      df.select(rf_tile_sum(rf_local_greater_equal($"three", $"three"))).first() should be(100.0)
      df.select(rf_tile_sum(rf_local_greater_equal($"three", $"six"))).first() should be(0.0)
      df.selectExpr("rf_tile_sum(rf_local_greater_equal(two, 1.9))").as[Double].first() should be(100.0)
      df.selectExpr("rf_tile_sum(rf_local_greater_equal(three, three))").as[Double].first() should be(100.0)
      checkDocs("rf_local_greater_equal")
    }

    it("should evaluate rf_local_equal") {
      val df = Seq((two, three, three)).toDF("two", "threeA", "threeB")
      df.select(rf_tile_sum(rf_local_equal($"two", 2))).first() should be(100.0)
      df.select(rf_tile_sum(rf_local_equal($"two", 2.1))).first() should be(0.0)
      df.select(rf_tile_sum(rf_local_equal($"two", $"threeA"))).first() should be(0.0)
      df.select(rf_tile_sum(rf_local_equal($"threeA", $"threeB"))).first() should be(100.0)
      df.selectExpr("rf_tile_sum(rf_local_equal(two, 1.9))").as[Double].first() should be(0.0)
      df.selectExpr("rf_tile_sum(rf_local_equal(threeA, threeB))").as[Double].first() should be(100.0)
      checkDocs("rf_local_equal")
    }

    it("should evaluate rf_local_unequal") {
      val df = Seq((two, three, three)).toDF("two", "threeA", "threeB")
      df.select(rf_tile_sum(rf_local_unequal($"two", 2))).first() should be(0.0)
      df.select(rf_tile_sum(rf_local_unequal($"two", 2.1))).first() should be(100.0)
      df.select(rf_tile_sum(rf_local_unequal($"two", $"threeA"))).first() should be(100.0)
      df.select(rf_tile_sum(rf_local_unequal($"threeA", $"threeB"))).first() should be(0.0)
      df.selectExpr("rf_tile_sum(rf_local_unequal(two, 1.9))").as[Double].first() should be(100.0)
      df.selectExpr("rf_tile_sum(rf_local_unequal(threeA, threeB))").as[Double].first() should be(0.0)
      checkDocs("rf_local_unequal")
    }
  }

  describe("tile min max and clip") {
    it("should support SQL API"){
      checkDocs("rf_local_min")
      checkDocs("rf_local_max")
      checkDocs("rf_local_clip")
    }
    it("should evaluate rf_local_min") {
      val df = Seq((randPRT, three)).toDF("tile", "three")
      val result1 = df.select(rf_local_min($"tile", $"three") as "t")
        .select(rf_tile_max($"t"))
        .first()
      result1 should be <= 3.0
    }
    it("should evaluate rf_local_min with scalar") {
      val df = Seq(randPRT).toDF("tile")
      val result1 = df.select(rf_local_min($"tile", 3) as "t")
        .select(rf_tile_max($"t"))
        .first()
      result1 should be <= 3.0
    }
    it("should evaluate rf_local_max") {
      val df = Seq((randPRT, three)).toDF("tile", "three")
      val result1 = df.select(rf_local_max($"tile", $"three") as "t")
        .select(rf_tile_min($"t"))
        .first()
      result1 should be >= 3.0
    }
    it("should evaluate rf_local_max with scalar") {
      val df = Seq(randPRT).toDF("tile")
      val result1 = df.select(rf_local_max($"tile", 3) as "t")
        .select(rf_tile_min($"t"))
        .first()
      result1 should be >= 3.0
    }
    it("should evaluate rf_local_clip"){
      val df = Seq((randPRT, two, six)).toDF("t", "two", "six")
      val result = df.select(rf_local_clip($"t", $"two", $"six") as "t")
        .select(rf_tile_min($"t") as "min", rf_tile_max($"t") as "max")
        .first()
      result(0) should be (2)
      result(1) should be (6)
    }
  }

  describe("conditional cell values"){

    it("should support SQL API") {
      checkDocs("rf_where")
    }

    it("should evaluate rf_where"){
      val df = Seq((randPRT, one, six)).toDF("t", "one", "six")
      val result = df.select(
        rf_for_all(
          rf_local_equal(
            rf_where(rf_local_greater($"t", 0), $"one", $"six") as "result",
            rf_local_add(
              rf_local_multiply(rf_local_greater($"t", 0), $"one"),
              rf_local_multiply(rf_local_less_equal($"t", 0), $"six")
            ) as "expected"
          )
        )
      )
      .first()

      result should be (true)

    }
  }

  describe("standardize and rescale") {

    it("should be accssible in SQL API"){
      checkDocs("rf_standardize")
      checkDocs("rf_rescale")
    }

    it("should evaluate rf_standardize") {
      import org.apache.spark.sql.functions.sqrt

      val df = Seq(randPRT, six, one).toDF("tile")
      val stats = df.agg(rf_agg_stats($"tile").alias("stat")).select($"stat.mean", sqrt($"stat.variance"))
        .first()
      val result = df.select(rf_standardize($"tile", stats.getAs[Double](0), stats.getAs[Double](1)) as "z")
        .agg(rf_agg_stats($"z") as "zstats")
        .select($"zstats.mean", $"zstats.variance")
        .first()

      result.getAs[Double](0) should be (0.0 +- 0.00001)
      result.getAs[Double](1) should be (1.0 +- 0.00001)
    }

    it("should evaluate rf_standardize with tile-level stats") {
      // this tile should already be Z distributed.
      val df = Seq(randDoubleTile).toDF("tile")
      val result = df.select(rf_standardize($"tile") as "z")
        .select(rf_tile_stats($"z") as "zstat")
        .select($"zstat.mean", $"zstat.variance")
        .first()

      result.getAs[Double](0) should be (0.0 +- 0.00001)
      result.getAs[Double](1) should be (1.0 +- 0.00001)
    }

    it("should evaluate rf_rescale") {
      import org.apache.spark.sql.functions.{min, max}
      val df = Seq(randPRT, six, one).toDF("tile")
      val stats = df.agg(rf_agg_stats($"tile").alias("stat")).select($"stat.min", $"stat.max")
        .first()

      val result = df.select(
        rf_rescale($"tile", stats.getDouble(0), stats.getDouble(1)).alias("t")
      )
        .agg(
          max(rf_tile_min($"t")),
          min(rf_tile_max($"t")),
          rf_agg_stats($"t").getField("min"),
          rf_agg_stats($"t").getField("max"))
        .first()

      result.getDouble(0) should be > (0.0)
      result.getDouble(1) should be < (1.0)
      result.getDouble(2) should be (0.0 +- 1e-7)
      result.getDouble(3) should be (1.0 +- 1e-7)

    }

    it("should evaluate rf_rescale with tile-level stats") {
      val df = Seq(randDoubleTile).toDF("tile")
      val result = df.select(rf_rescale($"tile") as "t")
        .select(rf_tile_stats($"t") as "tstat")
        .select($"tstat.min", $"tstat.max")
        .first()
      result.getAs[Double](0) should be (0.0 +- 1e-7)
      result.getAs[Double](1) should be (1.0 +- 1e-7)
    }

  }

  describe("raster metadata") {
    it("should get the TileDimensions of a Tile") {
      val t = Seq(randPRT).toDF("tile").select(rf_dimensions($"tile")).first()
      t should be(TileDimensions(randPRT.dimensions))
      checkDocs("rf_dimensions")
    }
    it("should get the Extent of a ProjectedRasterTile") {
      val e = Seq(randPRT).toDF("tile").select(rf_extent($"tile")).first()
      e should be(extent)
      checkDocs("rf_extent")
    }

    it("should get the CRS of a ProjectedRasterTile") {
      val e = Seq(randPRT).toDF("tile").select(rf_crs($"tile")).first()
      e should be(crs)
      checkDocs("rf_crs")
    }

    it("should parse a CRS from string") {
      val e = Seq(crs.toProj4String).toDF("crs").select(rf_crs($"crs")).first()
      e should be(crs)
    }

    it("should get the Geometry of a ProjectedRasterTile") {
      val g = Seq(randPRT).toDF("tile").select(rf_geometry($"tile")).first()
      g should be(extent.jtsGeom)
      checkDocs("rf_geometry")
    }

    it("should get the CRS of a RasteRef") {
      val e = Seq(Tuple1(rasterRef)).toDF("ref").select(rf_crs($"ref")).first()
      e should be(rasterRef.crs)
    }

    it("should get the Extent of a RasteRef") {
      val e = Seq(Tuple1(rasterRef)).toDF("ref").select(rf_extent($"ref")).first()
      e should be(rasterRef.extent)
    }
  }
  describe("per-tile stats") {
    it("should compute data cell counts") {
      val df = Seq(TestData.injectND(numND)(two)).toDF("two")
      df.select(rf_data_cells($"two")).first() shouldBe (cols * rows - numND).toLong

      val df2 = randNDTilesWithNull.toDF("tile")
      df2
        .select(rf_data_cells($"tile") as "cells")
        .agg(sum("cells"))
        .as[Long]
        .first() should be(expectedRandData)

      checkDocs("rf_data_cells")
    }
    it("should compute no-data cell counts") {
      val df = Seq(TestData.injectND(numND)(two)).toDF("two")
      df.select(rf_no_data_cells($"two")).first() should be(numND)

      val df2 = randNDTilesWithNull.toDF("tile")
      df2
        .select(rf_no_data_cells($"tile") as "cells")
        .agg(sum("cells"))
        .as[Long]
        .first() should be(expectedRandNoData)

      checkDocs("rf_no_data_cells")
    }

    it("should properly count data and nodata cells on constant tiles") {
      val rf = Seq(randPRT).toDF("tile")

      val df = rf
        .withColumn("make", rf_make_constant_tile(99, 3, 4, ByteConstantNoDataCellType))
        .withColumn("make2", rf_with_no_data($"make", 99))

      val counts = df
        .select(
          rf_no_data_cells($"make").alias("nodata1"),
          rf_data_cells($"make").alias("data1"),
          rf_no_data_cells($"make2").alias("nodata2"),
          rf_data_cells($"make2").alias("data2")
        )
        .as[(Long, Long, Long, Long)]
        .first()

      counts should be((0l, 12l, 12l, 0l))
    }

    it("should detect no-data tiles") {
      val df = Seq(nd).toDF("nd")
      df.select(rf_is_no_data_tile($"nd")).first() should be(true)
      val df2 = Seq(two).toDF("not_nd")
      df2.select(rf_is_no_data_tile($"not_nd")).first() should be(false)
      checkDocs("rf_is_no_data_tile")
    }

    it("should evaluate exists and for_all") {
      val df0 = Seq(zero).toDF("tile")
      df0.select(rf_exists($"tile")).first() should be(false)
      df0.select(rf_for_all($"tile")).first() should be(false)

      Seq(one).toDF("tile").select(rf_exists($"tile")).first() should be(true)
      Seq(one).toDF("tile").select(rf_for_all($"tile")).first() should be(true)

      val dfNd = Seq(TestData.injectND(1)(one)).toDF("tile")
      dfNd.select(rf_exists($"tile")).first() should be(true)
      dfNd.select(rf_for_all($"tile")).first() should be(false)

      checkDocs("rf_exists")
      checkDocs("rf_for_all")
    }
    it("should find the minimum cell value") {
      val min = randNDPRT.toArray().filter(c => isData(c)).min.toDouble
      val df = Seq(randNDPRT).toDF("rand")
      df.select(rf_tile_min($"rand")).first() should be(min)
      df.selectExpr("rf_tile_min(rand)").as[Double].first() should be(min)
      checkDocs("rf_tile_min")
    }

    it("should find the maximum cell value") {
      val max = randNDPRT.toArray().filter(c => isData(c)).max.toDouble
      val df = Seq(randNDPRT).toDF("rand")
      df.select(rf_tile_max($"rand")).first() should be(max)
      df.selectExpr("rf_tile_max(rand)").as[Double].first() should be(max)
      checkDocs("rf_tile_max")
    }
    it("should compute the tile mean cell value") {
      val values = randNDPRT.toArray().filter(c => isData(c))
      val mean = values.sum.toDouble / values.length
      val df = Seq(randNDPRT).toDF("rand")
      df.select(rf_tile_mean($"rand")).first() should be(mean)
      df.selectExpr("rf_tile_mean(rand)").as[Double].first() should be(mean)
      checkDocs("rf_tile_mean")
    }

    it("should compute the tile summary statistics") {
      val values = randNDPRT.toArray().filter(c => isData(c))
      val mean = values.sum.toDouble / values.length
      val df = Seq(randNDPRT).toDF("rand")
      val stats = df.select(rf_tile_stats($"rand")).first()
      stats.mean should be(mean +- 0.00001)

      val stats2 = df
        .selectExpr("rf_tile_stats(rand) as stats")
        .select($"stats".as[CellStatistics])
        .first()
      stats2 should be(stats)

      df.select(rf_tile_stats($"rand") as "stats")
        .select($"stats.mean")
        .as[Double]
        .first() should be(mean +- 0.00001)
      df.selectExpr("rf_tile_stats(rand) as stats")
        .select($"stats.no_data_cells")
        .as[Long]
        .first() should be <= (cols * rows - numND).toLong

      val df2 = randNDTilesWithNull.toDF("tile")
      df2
        .select(rf_tile_stats($"tile")("data_cells") as "cells")
        .agg(sum("cells"))
        .as[Long]
        .first() should be(expectedRandData)

      checkDocs("rf_tile_stats")
    }

    it("should compute the tile histogram") {
      val df = Seq(randNDPRT).toDF("rand")
      val h1 = df.select(rf_tile_histogram($"rand")).first()

      val h2 = df
        .selectExpr("rf_tile_histogram(rand) as hist")
        .select($"hist".as[CellHistogram])
        .first()

      h1 should be(h2)

      checkDocs("rf_tile_histogram")
    }
  }

  describe("conversion operations") {
    it("should convert tile into array") {
      val query = sql("""select rf_tile_to_array_int(
          |  rf_make_constant_tile(1, 10, 10, 'int8raw')
          |) as intArray
          |""".stripMargin)
      query.as[Array[Int]].first.sum should be(100)

      val tile = FloatConstantTile(1.1f, 10, 10, FloatCellType)
      val df = Seq[Tile](tile).toDF("tile")
      val arrayDF = df.select(rf_tile_to_array_double($"tile").as[Array[Double]])
      arrayDF.first().sum should be(110.0 +- 0.0001)

      checkDocs("rf_tile_to_array_int")
      checkDocs("rf_tile_to_array_double")
    }

    it("should convert an array into a tile") {
      val tile = TestData.randomTile(10, 10, FloatCellType)
      val df = Seq[Tile](tile, null).toDF("tile")
      val arrayDF = df.withColumn("tileArray", rf_tile_to_array_double($"tile"))

      val back = arrayDF.withColumn("backToTile", rf_array_to_tile($"tileArray", 10, 10))

      val result = back.select($"backToTile".as[Tile]).first

      assert(result.toArrayDouble() === tile.toArrayDouble())

      // Same round trip, but with SQL expression for rf_array_to_tile
      val resultSql = arrayDF.selectExpr("rf_array_to_tile(tileArray, 10, 10) as backToTile").as[Tile].first

      assert(resultSql.toArrayDouble() === tile.toArrayDouble())

      val hasNoData = back.withColumn("withNoData", rf_with_no_data($"backToTile", 0))

      val result2 = hasNoData.select($"withNoData".as[Tile]).first

      assert(result2.cellType.asInstanceOf[UserDefinedNoData[_]].noDataValue === 0)
    }

    it("should convert a CRS, Extent and Tile into `proj_raster` structure ") {
      implicit lazy val tripEnc = Encoders.tuple(extentEncoder, crsEncoder, singlebandTileEncoder)
      val expected = ProjectedRasterTile(randomTile(2, 2, ByteConstantNoDataCellType), extent, crs: CRS)
      val df = Seq((expected.extent, expected.crs, expected: Tile)).toDF("extent", "crs", "tile")
      val pr = df.select(rf_proj_raster($"tile", $"extent", $"crs")).first()
      pr should be(expected)
      checkDocs("rf_proj_raster")
    }
  }

  describe("ColorRampNames") {
    it("should have a list of color ramps") {
      ColorRampNames().length shouldBe >=(21)
    }
    it("should convert names to ColorRamps") {
      forEvery(ColorRampNames()) {
        case ColorRampNames(ramp) => ramp.numStops should be > (0)
        case o => fail(s"Expected $o to convert to color ramp")
      }
    }
    it("should return None on unrecognized names") {
      ColorRampNames.unapply("foobar") should be (None)
    }
  }
  
  describe("create encoded representation of images") {
    it("should create RGB composite") {
      val red = TestData.l8Sample(4).toProjectedRasterTile
      val green = TestData.l8Sample(3).toProjectedRasterTile
      val blue = TestData.l8Sample(2).toProjectedRasterTile

      val expected = ArrayMultibandTile(
        red.rescale(0, 255),
        green.rescale(0, 255),
        blue.rescale(0, 255)
      ).color()

      val df = Seq((red, green, blue)).toDF("red", "green", "blue")

      val expr = df.select(rf_rgb_composite($"red", $"green", $"blue")).as[ProjectedRasterTile]

      val nat_color = expr.first()

      checkDocs("rf_rgb_composite")
      assertEqual(nat_color.toArrayTile(), expected)
    }

    it("should create an RGB PNG image") {
      val red = TestData.l8Sample(4).toProjectedRasterTile
      val green = TestData.l8Sample(3).toProjectedRasterTile
      val blue = TestData.l8Sample(2).toProjectedRasterTile

      val df = Seq((red, green, blue)).toDF("red", "green", "blue")

      val expr = df.select(rf_render_png($"red", $"green", $"blue"))

      val pngData = expr.first()

      val image = ImageIO.read(new ByteArrayInputStream(pngData))
      image.getWidth should be(red.cols)
      image.getHeight should be(red.rows)
    }

    it("should create a color-ramp PNG image") {
      val red = TestData.l8Sample(4).toProjectedRasterTile

      val df = Seq(red).toDF("red")

      val expr = df.select(rf_render_png($"red", "HeatmapBlueToYellowToRedSpectrum"))

      val pngData = expr.first()

      val image = ImageIO.read(new ByteArrayInputStream(pngData))
      image.getWidth should be(red.cols)
      image.getHeight should be(red.rows)
    }
  }
}
