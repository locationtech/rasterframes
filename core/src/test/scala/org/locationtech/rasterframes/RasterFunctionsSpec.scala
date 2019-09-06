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

import java.io.ByteArrayInputStream

import geotrellis.raster
import geotrellis.raster._
import geotrellis.raster.render.ColorRamps
import geotrellis.raster.testkit.RasterMatchers
import javax.imageio.ImageIO
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._
import org.locationtech.rasterframes.expressions.accessors.ExtractTile
import org.locationtech.rasterframes.model.TileDimensions
import org.locationtech.rasterframes.stats._
import org.locationtech.rasterframes.tiles.ProjectedRasterTile

class RasterFunctionsSpec extends TestEnvironment with RasterMatchers {
  import TestData._
  import spark.implicits._

  implicit val pairEnc = Encoders.tuple(ProjectedRasterTile.prtEncoder, ProjectedRasterTile.prtEncoder)
  implicit val tripEnc = Encoders.tuple(ProjectedRasterTile.prtEncoder, ProjectedRasterTile.prtEncoder, ProjectedRasterTile.prtEncoder)

  describe("constant tile generation operations") {
    val dim = 2
    val rows = 2

    it("should create a ones tile") {
      val df = (0 until rows).toDF("id")
        .withColumn("const", rf_make_ones_tile(dim, dim, IntConstantNoDataCellType))
      val result = df.select(rf_tile_sum($"const") as "ts").agg(sum("ts")).as[Double].first()
      result should be (dim * dim * rows)
    }

    it("should create a zeros tile") {
      val df = (0 until rows).toDF("id")
        .withColumn("const", rf_make_zeros_tile(dim, dim, FloatConstantNoDataCellType))
      val result = df.select(rf_tile_sum($"const") as "ts").agg(sum("ts")).as[Double].first()
      result should be (0)
    }

    it("should create an arbitrary constant tile") {
      val value = 4
      val df = (0 until rows).toDF("id")
        .withColumn("const", rf_make_constant_tile(value, dim, dim, ByteConstantNoDataCellType))
      val result = df.select(rf_tile_sum($"const") as "ts").agg(sum("ts")).as[Double].first()
      result should be (dim * dim * rows * value)
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

      ct3.cellType should be (UShortUserDefinedNoDataCellType(512))
      ct2.cellType should be (FloatConstantNoDataCellType)

      val (cnt3, cnt2) = ct.select(rf_no_data_cells($"three"), rf_no_data_cells($"two")).as[(Long, Long)].first()

      cnt3 should be (7)
      cnt2 should be (12)

      checkDocs("rf_convert_cell_type")
    }
    it("should change NoData value") {
      val df = Seq((TestData.injectND(7)(three), TestData.injectND(12)(two))).toDF("three", "two")

      val ndCT = df.select(
        rf_with_no_data($"three", 3) as "three",
        rf_with_no_data($"two", 2.0) as "two"
      )

      val (cnt3, cnt2) = ndCT.select(rf_no_data_cells($"three"), rf_no_data_cells($"two")).as[(Long, Long)].first()

      cnt3 should be ((cols * rows) - 7)
      cnt2 should be ((cols * rows) - 12)

      checkDocs("rf_with_no_data")

      // Should maintain original cell type.
      ndCT.select(rf_cell_type($"two")).first().withDefaultNoData() should be(ct.withDefaultNoData())
    }
  }

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

      assertEqual(df.selectExpr("rf_local_multiply(rf_local_divide(six, 2.0), two)")
        .as[ProjectedRasterTile].first(), six)

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

  describe("raster metadata") {
    it("should get the TileDimensions of a Tile") {
      val t = Seq(randPRT).toDF("tile").select(rf_dimensions($"tile")).first()
      t should be (TileDimensions(randPRT.dimensions))
      checkDocs("rf_dimensions")
    }
    it("should get the Extent of a ProjectedRasterTile") {
      val e = Seq(randPRT).toDF("tile").select(rf_extent($"tile")).first()
      e should be (extent)
      checkDocs("rf_extent")
    }

    it("should get the CRS of a ProjectedRasterTile") {
      val e = Seq(randPRT).toDF("tile").select(rf_crs($"tile")).first()
      e should be (crs)
      checkDocs("rf_crs")
    }

    it("should get the Geometry of a ProjectedRasterTile") {
      val g = Seq(randPRT).toDF("tile").select(rf_geometry($"tile")).first()
      g should be (extent.jtsGeom)
      checkDocs("rf_geometry")
    }
  }

  describe("per-tile stats") {
    it("should compute data cell counts") {
      val df = Seq(TestData.injectND(numND)(two)).toDF("two")
      df.select(rf_data_cells($"two")).first() shouldBe (cols * rows - numND).toLong

      val df2 = randNDTilesWithNull.toDF("tile")
      df2.select(rf_data_cells($"tile") as "cells")
        .agg(sum("cells"))
        .as[Long]
        .first() should be (expectedRandData)

      checkDocs("rf_data_cells")
    }
    it("should compute no-data cell counts") {
      val df = Seq(TestData.injectND(numND)(two)).toDF("two")
      df.select(rf_no_data_cells($"two")).first() should be(numND)

      val df2 = randNDTilesWithNull.toDF("tile")
      df2.select(rf_no_data_cells($"tile") as "cells")
        .agg(sum("cells"))
        .as[Long]
        .first() should be (expectedRandNoData)

      checkDocs("rf_no_data_cells")
    }

    it("should properly count data and nodata cells on constant tiles") {
      val rf = Seq(randPRT).toDF("tile")

      val df = rf
        .withColumn("make", rf_make_constant_tile(99, 3, 4, ByteConstantNoDataCellType))
        .withColumn("make2", rf_with_no_data($"make", 99))

      val counts = df.select(
        rf_no_data_cells($"make").alias("nodata1"),
        rf_data_cells($"make").alias("data1"),
        rf_no_data_cells($"make2").alias("nodata2"),
        rf_data_cells($"make2").alias("data2")
      ).as[(Long, Long, Long, Long)].first()

      counts should be ((0l, 12l, 12l, 0l))
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
      val min = randNDPRT.toArray().filter(c => raster.isData(c)).min.toDouble
      val df = Seq(randNDPRT).toDF("rand")
      df.select(rf_tile_min($"rand")).first() should be(min)
      df.selectExpr("rf_tile_min(rand)").as[Double].first() should be(min)
      checkDocs("rf_tile_min")
    }

    it("should find the maximum cell value") {
      val max = randNDPRT.toArray().filter(c => raster.isData(c)).max.toDouble
      val df = Seq(randNDPRT).toDF("rand")
      df.select(rf_tile_max($"rand")).first() should be(max)
      df.selectExpr("rf_tile_max(rand)").as[Double].first() should be(max)
      checkDocs("rf_tile_max")
    }
    it("should compute the tile mean cell value") {
      val values = randNDPRT.toArray().filter(c => raster.isData(c))
      val mean = values.sum.toDouble / values.length
      val df = Seq(randNDPRT).toDF("rand")
      df.select(rf_tile_mean($"rand")).first() should be(mean)
      df.selectExpr("rf_tile_mean(rand)").as[Double].first() should be(mean)
      checkDocs("rf_tile_mean")
    }

    it("should compute the tile summary statistics") {
      val values = randNDPRT.toArray().filter(c => raster.isData(c))
      val mean = values.sum.toDouble / values.length
      val df = Seq(randNDPRT).toDF("rand")
      val stats = df.select(rf_tile_stats($"rand")).first()
      stats.mean should be (mean +- 0.00001)

      val stats2 = df.selectExpr("rf_tile_stats(rand) as stats")
        .select($"stats".as[CellStatistics])
        .first()
      stats2 should be (stats)

      df.select(rf_tile_stats($"rand") as "stats")
        .select($"stats.mean").as[Double]
        .first() should be(mean +- 0.00001)
      df.selectExpr("rf_tile_stats(rand) as stats")
        .select($"stats.no_data_cells").as[Long]
        .first() should be <= (cols * rows - numND).toLong

      val df2 = randNDTilesWithNull.toDF("tile")
      df2
        .select(rf_tile_stats($"tile")("data_cells") as "cells")
        .agg(sum("cells"))
        .as[Long]
        .first() should be (expectedRandData)

      checkDocs("rf_tile_stats")
    }

    it("should compute the tile histogram") {
      val df = Seq(randNDPRT).toDF("rand")
      val h1 = df.select(rf_tile_histogram($"rand")).first()

      val h2 = df.selectExpr("rf_tile_histogram(rand) as hist")
        .select($"hist".as[CellHistogram])
        .first()

      h1 should be (h2)

      checkDocs("rf_tile_histogram")
    }
  }

  describe("aggregate statistics") {
    it("should count data cells") {
      val df = randNDTilesWithNull.filter(_ != null).toDF("tile")
      df.select(rf_agg_data_cells($"tile")).first() should be (expectedRandData)
      df.selectExpr("rf_agg_data_cells(tile)").as[Long].first() should be (expectedRandData)

      checkDocs("rf_agg_data_cells")
    }
    it("should count no-data cells") {
      val df = randNDTilesWithNull.toDF("tile")
      df.select(rf_agg_no_data_cells($"tile")).first() should be (expectedRandNoData)
      df.selectExpr("rf_agg_no_data_cells(tile)").as[Long].first() should be (expectedRandNoData)
      checkDocs("rf_agg_no_data_cells")
    }

    it("should compute aggregate statistics") {
      val df = randNDTilesWithNull.toDF("tile")

      df
        .select(rf_agg_stats($"tile") as "stats")
        .select("stats.data_cells", "stats.no_data_cells")
        .as[(Long, Long)]
        .first() should be ((expectedRandData, expectedRandNoData))
      df.selectExpr("rf_agg_stats(tile) as stats")
        .select("stats.data_cells")
        .as[Long]
        .first() should be (expectedRandData)

      checkDocs("rf_agg_stats")
    }

    it("should compute a aggregate histogram") {
      val df = randNDTilesWithNull.toDF("tile")
      val hist1 = df.select(rf_agg_approx_histogram($"tile")).first()
      val hist2 = df.selectExpr("rf_agg_approx_histogram(tile) as hist")
        .select($"hist".as[CellHistogram])
        .first()
      hist1 should be (hist2)
      checkDocs("rf_agg_approx_histogram")
    }

    it("should compute local statistics") {
      val df = randNDTilesWithNull.toDF("tile")
      val stats1 = df.select(rf_agg_local_stats($"tile"))
        .first()
      val stats2 = df.selectExpr("rf_agg_local_stats(tile) as stats")
          .select($"stats".as[LocalCellStatistics])
          .first()

      stats1 should be (stats2)
      checkDocs("rf_agg_local_stats")
    }

    it("should compute local min") {
      val df = Seq(two, three, one, six).toDF("tile")
      df.select(rf_agg_local_min($"tile")).first() should be(one.toArrayTile())
      df.selectExpr("rf_agg_local_min(tile)").as[Tile].first() should be(one.toArrayTile())
      checkDocs("rf_agg_local_min")
    }

    it("should compute local max") {
      val df = Seq(two, three, one, six).toDF("tile")
      df.select(rf_agg_local_max($"tile")).first() should be(six.toArrayTile())
      df.selectExpr("rf_agg_local_max(tile)").as[Tile].first() should be(six.toArrayTile())
      checkDocs("rf_agg_local_max")
    }

    it("should compute local mean") {
      checkDocs("rf_agg_local_mean")
      val df = Seq(two, three, one, six).toDF("tile")
          .withColumn("id", monotonically_increasing_id())

      df.select(rf_agg_local_mean($"tile")).first() should be(three.toArrayTile())

      df.selectExpr("rf_agg_local_mean(tile)").as[Tile].first() should be(three.toArrayTile())

      noException should be thrownBy {
        df.groupBy($"id")
          .agg(rf_agg_local_mean($"tile"))
          .collect()
      }
    }

    it("should compute local data cell counts") {
      val df = Seq(two, randNDPRT, nd).toDF("tile")
      val t1 = df.select(rf_agg_local_data_cells($"tile")).first()
      val t2 = df.selectExpr("rf_agg_local_data_cells(tile) as cnt").select($"cnt".as[Tile]).first()
      t1 should be (t2)
      checkDocs("rf_agg_local_data_cells")
    }

    it("should compute local no-data cell counts") {
      val df = Seq(two, randNDPRT, nd).toDF("tile")
      val t1 = df.select(rf_agg_local_no_data_cells($"tile")).first()
      val t2 = df.selectExpr("rf_agg_local_no_data_cells(tile) as cnt").select($"cnt".as[Tile]).first()
      t1 should be (t2)
      val t3 = df.select(rf_local_add(rf_agg_local_data_cells($"tile"), rf_agg_local_no_data_cells($"tile"))).as[Tile].first()
      t3 should be(three.toArrayTile())
      checkDocs("rf_agg_local_no_data_cells")
    }
  }

  describe("array operations") {
    it("should convert tile into array") {
      val query = sql(
        """select rf_tile_to_array_int(
          |  rf_make_constant_tile(1, 10, 10, 'int8raw')
          |) as intArray
          |""".stripMargin)
      query.as[Array[Int]].first.sum should be (100)

      val tile = FloatConstantTile(1.1f, 10, 10, FloatCellType)
      val df = Seq[Tile](tile).toDF("tile")
      val arrayDF = df.select(rf_tile_to_array_double($"tile").as[Array[Double]])
      arrayDF.first().sum should be (110.0 +- 0.0001)

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
  }

  describe("analytical transformations") {
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

    it("should inverse mask tile by another identified by specified value") {
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
        rf_inverse_mask_by_value($"tile", $"mask", lit(mask_value)))

      val result = withMasked.agg(rf_agg_no_data_cells($"tile") < rf_agg_no_data_cells($"masked")).as[Boolean]

      result.first() should be(true)
      checkDocs("rf_inverse_mask_by_value")
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

    it("should take logarithms positive cell values"){
      // rf_log10 1000 == 3
      val thousand = TestData.projectedRasterTile(cols, rows, 1000, extent, crs, ShortConstantNoDataCellType)
      val threesDouble = TestData.projectedRasterTile(cols, rows, 3.0, extent, crs, DoubleConstantNoDataCellType)
      val zerosDouble = TestData.projectedRasterTile(cols, rows, 0.0, extent, crs, DoubleConstantNoDataCellType)

      val df1 = Seq(thousand).toDF("tile")
      assertEqual(df1.select(rf_log10($"tile")).as[ProjectedRasterTile].first(), threesDouble)

      // ln random tile == rf_log10 random tile / rf_log10(e); random tile square to ensure all positive cell values
      val df2 = Seq(randPositiveDoubleTile).toDF("tile")
      val log10e = math.log10(math.E)
      assertEqual(df2.select(rf_log($"tile")).as[ProjectedRasterTile].first(),
                  df2.select(rf_log10($"tile")).as[ProjectedRasterTile].first() / log10e)

      lazy val maybeZeros = df2
        .selectExpr(s"rf_local_subtract(rf_log(tile), rf_local_divide(rf_log10(tile), ${log10e}))")
        .as[ProjectedRasterTile].first()
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
      val zero_float =TestData.projectedRasterTile(cols, rows, 0.0, extent, crs, DoubleConstantNoDataCellType)

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
      assertEqual(
        df.select(rf_exp2(rf_log2($"tile"))).as[ProjectedRasterTile].first(),
        six)

      // base 10
      assertEqual(
        df.select(rf_exp10(rf_log10($"tile"))).as[ProjectedRasterTile].first(),
        six)

      // plus/minus 1
      assertEqual(
        df.select(rf_expm1(rf_log1p($"tile"))).as[ProjectedRasterTile].first(),
        six)

      // SQL
      assertEqual(
        df.selectExpr("rf_exp(rf_log(tile))").as[ProjectedRasterTile].first(),
        six)

      // SQL base 10
      assertEqual(
        df.selectExpr("rf_exp10(rf_log10(tile))").as[ProjectedRasterTile].first(),
        six)

      // SQL base 2
      assertEqual(
        df.selectExpr("rf_exp2(rf_log2(tile))").as[ProjectedRasterTile].first(),
        six)

      // SQL rf_expm1
      assertEqual(
        df.selectExpr("rf_expm1(rf_log1p(tile))").as[ProjectedRasterTile].first(),
        six)

      checkDocs("rf_exp")
      checkDocs("rf_exp10")
      checkDocs("rf_exp2")
      checkDocs("rf_expm1")

    }
  }
  it("should resample") {
    def lowRes = {
      def base = ArrayTile(Array(1,2,3,4), 2, 2)
      ProjectedRasterTile(base.convert(ct), extent, crs)
    }
    def upsampled = {
      def base = ArrayTile(Array(
        1,1,2,2,
        1,1,2,2,
        3,3,4,4,
        3,3,4,4
      ), 4, 4)
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

    val expr = df.select(rf_render_png($"red", ColorRamps.HeatmapBlueToYellowToRedSpectrum))

    val pngData = expr.first()

    val image = ImageIO.read(new ByteArrayInputStream(pngData))
    image.getWidth should be(red.cols)
    image.getHeight should be(red.rows)
  }
  it("should interpret cell values with a specified cell type") {
    checkDocs("rf_interpret_cell_type_as")
    val df = Seq(randNDPRT).toDF("t")
      .withColumn("tile", rf_interpret_cell_type_as($"t", "int8raw"))
    val resultTile = df.select("tile").as[Tile].first()

    resultTile.cellType should be (CellType.fromName("int8raw"))
    // should have same number of values that are -2 the old ND
    val countOldNd = df.select(
      rf_tile_sum(rf_local_equal($"tile", ct.noDataValue)),
      rf_no_data_cells($"t")
    ).first()
    countOldNd._1 should be (countOldNd._2)

    // should not have no data any more (raw type)
    val countNewNd = df.select(rf_no_data_cells($"tile")).first()
    countNewNd should be (0L)

  }

  it("should return local data and nodata"){
    checkDocs("rf_local_data")
    checkDocs("rf_local_no_data")

    val df = Seq(randNDPRT).toDF("t")
      .withColumn("ld", rf_local_data($"t"))
      .withColumn("lnd", rf_local_no_data($"t"))

    val ndResult = df.select($"lnd").as[Tile].first()
    ndResult should be (randNDPRT.localUndefined())

    val dResult = df.select($"ld").as[Tile].first()
    dResult should be (randNDPRT.localDefined())
  }
}
