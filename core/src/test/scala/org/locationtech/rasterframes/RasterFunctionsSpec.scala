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

import geotrellis.proj4.LatLng
import geotrellis.raster
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.raster._
import geotrellis.vector.Extent
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._
import org.locationtech.rasterframes.expressions.accessors.ExtractTile
import org.locationtech.rasterframes.stats._
import org.locationtech.rasterframes.tiles.ProjectedRasterTile
import org.scalatest.{FunSpec, Matchers}

class RasterFunctionsSpec extends FunSpec
  with TestEnvironment with Matchers with RasterMatchers {
  import spark.implicits._

  val extent = Extent(10, 20, 30, 40)
  val crs = LatLng
  val ct = ByteUserDefinedNoDataCellType(-2)
  val cols = 10
  val rows = cols
  val tileSize = cols * rows
  val tileCount = 10
  val numND = 4
  lazy val zero = TestData.projectedRasterTile(cols, rows, 0, extent, crs, ct)
  lazy val one = TestData.projectedRasterTile(cols, rows, 1, extent, crs, ct)
  lazy val two = TestData.projectedRasterTile(cols, rows, 2, extent, crs, ct)
  lazy val three = TestData.projectedRasterTile(cols, rows, 3, extent, crs, ct)
  lazy val six = ProjectedRasterTile(three * two, three.extent, three.crs)
  lazy val nd = TestData.projectedRasterTile(cols, rows, -2, extent, crs, ct)
  lazy val randTile = TestData.projectedRasterTile(cols, rows, scala.util.Random.nextInt(), extent, crs, ct)
  lazy val randNDTile  = TestData.injectND(numND)(randTile)

  lazy val randDoubleTile = TestData.projectedRasterTile(cols, rows, scala.util.Random.nextGaussian(), extent, crs, DoubleConstantNoDataCellType)
  lazy val randDoubleNDTile  = TestData.injectND(numND)(randDoubleTile)
  lazy val randPositiveDoubleTile = TestData.projectedRasterTile(cols, rows, scala.util.Random.nextDouble() + 1e-6, extent, crs, DoubleConstantNoDataCellType)

  val expectedRandNoData: Long = numND * tileCount.toLong
  val expectedRandData: Long = cols * rows * tileCount - expectedRandNoData
  lazy val randNDTilesWithNull = Seq.fill[Tile](tileCount)(TestData.injectND(numND)(
    TestData.randomTile(cols, rows, UByteConstantNoDataCellType)
  )).map(ProjectedRasterTile(_, extent, crs)) :+ null

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
    it("should detect no-data tiles") {
      val df = Seq(nd).toDF("nd")
      df.select(rf_is_no_data_tile($"nd")).first() should be(true)
      val df2 = Seq(two).toDF("not_nd")
      df2.select(rf_is_no_data_tile($"not_nd")).first() should be(false)
      checkDocs("rf_is_no_data_tile")
    }
    it("should find the minimum cell value") {
      val min = randNDTile.toArray().filter(c => raster.isData(c)).min.toDouble
      val df = Seq(randNDTile).toDF("rand")
      df.select(rf_tile_min($"rand")).first() should be(min)
      df.selectExpr("rf_tile_min(rand)").as[Double].first() should be(min)
      checkDocs("rf_tile_min")
    }

    it("should find the maximum cell value") {
      val max = randNDTile.toArray().filter(c => raster.isData(c)).max.toDouble
      val df = Seq(randNDTile).toDF("rand")
      df.select(rf_tile_max($"rand")).first() should be(max)
      df.selectExpr("rf_tile_max(rand)").as[Double].first() should be(max)
      checkDocs("rf_tile_max")
    }
    it("should compute the tile mean cell value") {
      val values = randNDTile.toArray().filter(c => raster.isData(c))
      val mean = values.sum.toDouble / values.length
      val df = Seq(randNDTile).toDF("rand")
      df.select(rf_tile_mean($"rand")).first() should be(mean)
      df.selectExpr("rf_tile_mean(rand)").as[Double].first() should be(mean)
      checkDocs("rf_tile_mean")
    }

    it("should compute the tile summary statistics") {
      val values = randNDTile.toArray().filter(c => raster.isData(c))
      val mean = values.sum.toDouble / values.length
      val df = Seq(randNDTile).toDF("rand")
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
      val df = Seq(randNDTile).toDF("rand")
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

    it("should compute local data cell counts") {
      val df = Seq(two, randNDTile, nd).toDF("tile")
      val t1 = df.select(rf_agg_local_data_cells($"tile")).first()
      val t2 = df.selectExpr("rf_agg_local_data_cells(tile) as cnt").select($"cnt".as[Tile]).first()
      t1 should be (t2)
      checkDocs("rf_agg_local_data_cells")
    }

    it("should compute local no-data cell counts") {
      val df = Seq(two, randNDTile, nd).toDF("tile")
      val t1 = df.select(rf_agg_local_no_data_cells($"tile")).first()
      val t2 = df.selectExpr("rf_agg_local_no_data_cells(tile) as cnt").select($"cnt".as[Tile]).first()
      t1 should be (t2)
      val t3 = df.select(rf_local_add(rf_agg_local_data_cells($"tile"), rf_agg_local_no_data_cells($"tile"))).first()
      t3 should be(three.toArrayTile())
      checkDocs("rf_agg_local_no_data_cells")
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

    it("should rf_mask one tile against another") {
      val df = Seq[Tile](randTile).toDF("tile")

      val withMask = df.withColumn("rf_mask",
        rf_convert_cell_type(
          rf_local_greater($"tile", 50),
          "uint8")
      )

      val withMasked = withMask.withColumn("masked",
        rf_mask($"tile", $"rf_mask"))

      val result = withMasked.agg(rf_agg_no_data_cells($"tile") < rf_agg_no_data_cells($"masked")).as[Boolean]

      result.first() should be(true)

      checkDocs("rf_mask")
    }

    it("should inverse rf_mask one tile against another") {
      val df = Seq[Tile](randTile).toDF("tile")

      val baseND = df.select(rf_agg_no_data_cells($"tile")).first()

      val withMask = df.withColumn("rf_mask",
        rf_convert_cell_type(
          rf_local_greater($"tile", 50),
          "uint8"
        )
      )

      val withMasked = withMask
        .withColumn("masked", rf_mask($"tile", $"rf_mask"))
        .withColumn("inv_masked", rf_inverse_mask($"tile", $"rf_mask"))

      val result = withMasked.agg(rf_agg_no_data_cells($"masked") + rf_agg_no_data_cells($"inv_masked")).as[Long]

      result.first() should be(tileSize + baseND)

      checkDocs("rf_inverse_mask")
    }

    it("should rf_mask tile by another identified by specified value") {
      val df = Seq[Tile](randTile).toDF("tile")
      val mask_value = 4

      val withMask = df.withColumn("rf_mask",
        rf_local_multiply(rf_convert_cell_type(
          rf_local_greater($"tile", 50),
          "uint8"),
          lit(mask_value)
        )
      )

      val withMasked = withMask.withColumn("masked",
        rf_mask_by_value($"tile", $"rf_mask", lit(mask_value)))

      val result = withMasked.agg(rf_agg_no_data_cells($"tile") < rf_agg_no_data_cells($"masked")).as[Boolean]

      result.first() should be(true)
      checkDocs("rf_mask_by_value")
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
}
