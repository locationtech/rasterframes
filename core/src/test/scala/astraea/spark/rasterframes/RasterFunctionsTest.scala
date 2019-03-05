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
import astraea.spark.rasterframes.TestData.injectND
import astraea.spark.rasterframes.expressions.accessors.ExtractTile
import astraea.spark.rasterframes.tiles.ProjectedRasterTile
import geotrellis.proj4.LatLng
import geotrellis.raster
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.raster.{ByteUserDefinedNoDataCellType, DoubleConstantNoDataCellType, Tile, UByteConstantNoDataCellType}
import geotrellis.vector.Extent
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._
import org.scalatest.{FunSpec, Matchers}

class RasterFunctionsTest extends FunSpec
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
  val one = TestData.projectedRasterTile(cols, rows, 1, extent, crs, ct)
  val two = TestData.projectedRasterTile(cols, rows, 2, extent, crs, ct)
  val three = TestData.projectedRasterTile(cols, rows, 3, extent, crs, ct)
  val six = ProjectedRasterTile(three * two, three.extent, three.crs)
  val nd = TestData.projectedRasterTile(cols, rows, -2, extent, crs, ct)
  val randTile = TestData.projectedRasterTile(cols, rows, scala.util.Random.nextInt(), extent, crs, ct)
  val randNDTile  = TestData.injectND(numND)(randTile)

  val randDoubleTile = TestData.projectedRasterTile(cols, rows, scala.util.Random.nextGaussian(), extent, crs, DoubleConstantNoDataCellType)
  val randDoubleNDTile  = TestData.injectND(numND)(randDoubleTile)

  val expectedRandNoData: Long = numND * tileCount
  val expectedRandData: Long = cols * rows * tileCount - expectedRandNoData
  val randNDTilesWithNull = Seq.fill[Tile](tileCount)(injectND(numND)(
    TestData.randomTile(cols, rows, UByteConstantNoDataCellType)
  )).map(ProjectedRasterTile(_, extent, crs)) :+ null



  implicit val pairEnc = Encoders.tuple(ProjectedRasterTile.prtEncoder, ProjectedRasterTile.prtEncoder)
  implicit val tripEnc = Encoders.tuple(ProjectedRasterTile.prtEncoder, ProjectedRasterTile.prtEncoder, ProjectedRasterTile.prtEncoder)


  describe("arithmetic tile operations") {
    it("should local_add") {
      val df = Seq((one, two)).toDF("one", "two")

      val maybeThree = df.select(local_add($"one", $"two")).as[ProjectedRasterTile]
      maybeThree.show(false)
      assertEqual(maybeThree.first(), three)

      assertEqual(df.selectExpr("rf_local_add(one, two)").as[ProjectedRasterTile].first(), three)

      val maybeThreeTile = df.select(local_add(ExtractTile($"one"), ExtractTile($"two"))).as[Tile]
      assertEqual(maybeThreeTile.first(), three.toArrayTile())
      checkDocs("rf_local_add")
    }

    it("should local_subtract") {
      val df = Seq((three, two)).toDF("three", "two")
      val maybeOne = df.select(local_subtract($"three", $"two")).as[ProjectedRasterTile]
      assertEqual(maybeOne.first(), one)

      assertEqual(df.selectExpr("rf_local_subtract(three, two)").as[ProjectedRasterTile].first(), one)

      val maybeOneTile =
        df.select(local_subtract(ExtractTile($"three"), ExtractTile($"two"))).as[Tile]
      assertEqual(maybeOneTile.first(), one.toArrayTile())
      checkDocs("rf_local_subtract")
    }

    it("should local_multiply") {
      val df = Seq((three, two)).toDF("three", "two")

      val maybeSix = df.select(local_multiply($"three", $"two")).as[ProjectedRasterTile]
      assertEqual(maybeSix.first(), six)

      assertEqual(df.selectExpr("rf_local_multiply(three, two)").as[ProjectedRasterTile].first(), six)

      val maybeSixTile =
        df.select(local_multiply(ExtractTile($"three"), ExtractTile($"two"))).as[Tile]
      assertEqual(maybeSixTile.first(), six.toArrayTile())
      checkDocs("rf_local_multiply")
    }

    it("should local_divide") {
      val df = Seq((six, two)).toDF("six", "two")
      val maybeThree = df.select(local_divide($"six", $"two")).as[ProjectedRasterTile]
      assertEqual(maybeThree.first(), three)

      assertEqual(df.selectExpr("rf_local_divide(six, two)").as[ProjectedRasterTile].first(), three)

      val maybeThreeTile =
        df.select(local_divide(ExtractTile($"six"), ExtractTile($"two"))).as[Tile]
      assertEqual(maybeThreeTile.first(), three.toArrayTile())
      checkDocs("rf_local_divide")
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
      checkDocs("rf_local_less")
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
      checkDocs("rf_local_less_equal")
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
      checkDocs("rf_local_greater")
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
      checkDocs("rf_local_greater_equal")
    }

    it("should evaluate local_equal") {
      val df = Seq((two, three, three)).toDF("two", "threeA", "threeB")
      df.select(tile_sum(local_equal($"two", 2))).first() should be(100.0)
      df.select(tile_sum(local_equal($"two", 2.1))).first() should be(0.0)
      df.select(tile_sum(local_equal($"two", $"threeA"))).first() should be(0.0)
      df.select(tile_sum(local_equal($"threeA", $"threeB"))).first() should be(100.0)
      df.selectExpr("rf_tile_sum(rf_local_equal(two, 1.9))").as[Double].first() should be(0.0)
      df.selectExpr("rf_tile_sum(rf_local_equal(threeA, threeB))").as[Double].first() should be(100.0)
      checkDocs("rf_local_equal")
    }

    it("should evaluate local_unequal") {
      val df = Seq((two, three, three)).toDF("two", "threeA", "threeB")
      df.select(tile_sum(local_unequal($"two", 2))).first() should be(0.0)
      df.select(tile_sum(local_unequal($"two", 2.1))).first() should be(100.0)
      df.select(tile_sum(local_unequal($"two", $"threeA"))).first() should be(100.0)
      df.select(tile_sum(local_unequal($"threeA", $"threeB"))).first() should be(0.0)
      df.selectExpr("rf_tile_sum(rf_local_unequal(two, 1.9))").as[Double].first() should be(100.0)
      df.selectExpr("rf_tile_sum(rf_local_unequal(threeA, threeB))").as[Double].first() should be(0.0)
      checkDocs("rf_local_unequal")
    }
  }

  describe("per-tile stats") {
    it("should compute data cell counts") {
      val df = Seq(TestData.injectND(numND)(two)).toDF("two")
      df.select(data_cells($"two")).first() shouldBe (cols * rows - numND).toLong

      val df2 = randNDTilesWithNull.toDF("tile")
      df2.select(data_cells($"tile") as "cells")
        .agg(sum("cells"))
        .as[Long]
        .first() should be (expectedRandData)

      checkDocs("rf_data_cells")
    }
    it("should compute no-data cell counts") {
      val df = Seq(TestData.injectND(numND)(two)).toDF("two")
      df.select(no_data_cells($"two")).first() should be(numND)

      val df2 = randNDTilesWithNull.toDF("tile")
      df2.select(no_data_cells($"tile") as "cells")
        .agg(sum("cells"))
        .as[Long]
        .first() should be (expectedRandNoData)

      checkDocs("rf_no_data_cells")
    }
    it("should detect no-data tiles") {
      val df = Seq(nd).toDF("nd")
      df.select(is_no_data_tile($"nd")).first() should be(true)
      val df2 = Seq(two).toDF("not_nd")
      df2.select(is_no_data_tile($"not_nd")).first() should be(false)
      checkDocs("rf_is_no_data_tile")
    }
    it("should find the minimum cell value") {
      val min = randNDTile.toArray().filter(c => raster.isData(c)).min.toDouble
      val df = Seq(randNDTile).toDF("rand")
      df.select(tile_min($"rand")).first() should be(min)
      df.selectExpr("rf_tile_min(rand)").as[Double].first() should be(min)
      checkDocs("rf_tile_min")
    }

    it("should find the maximum cell value") {
      val max = randNDTile.toArray().filter(c => raster.isData(c)).max.toDouble
      val df = Seq(randNDTile).toDF("rand")
      df.select(tile_max($"rand")).first() should be(max)
      df.selectExpr("rf_tile_max(rand)").as[Double].first() should be(max)
      checkDocs("rf_tile_max")
    }
    it("should compute the tile mean cell value") {
      val values = randNDTile.toArray().filter(c => raster.isData(c))
      val mean = values.sum.toDouble / values.length
      val df = Seq(randNDTile).toDF("rand")
      df.select(tile_mean($"rand")).first() should be(mean)
      df.selectExpr("rf_tile_mean(rand)").as[Double].first() should be(mean)
      checkDocs("rf_tile_mean")
    }

    it("should compute the tile summary statistics") {
      val values = randNDTile.toArray().filter(c => raster.isData(c))
      val mean = values.sum.toDouble / values.length
      val df = Seq(randNDTile).toDF("rand")
      val stats = df.select(tile_stats($"rand")).first()
      stats.mean should be (mean +- 0.00001)

      df.select(tile_stats($"rand") as "stats")
        .select($"stats.mean").as[Double]
        .first() should be(mean +- 0.00001)
      df.selectExpr("rf_tile_stats(rand) as stats")
        .select($"stats.no_data_cells").as[Long]
        .first() should be <= (cols * rows - numND).toLong

      val df2 = randNDTilesWithNull.toDF("tile")
      df2
        .select(tile_stats($"tile")("data_cells") as "cells")
        .agg(sum("cells"))
        .as[Long]
        .first() should be (expectedRandData)

      checkDocs("rf_tile_stats")
    }

    it("should compute the tile histogram") {
      val values = randNDTile.toArray().filter(c => raster.isData(c))
      val mean = values.sum.toDouble / values.length
      val df = Seq(randNDTile).toDF("rand")
      val hist = df.select(tile_histogram($"rand")).first()

      hist.stats.mean should be (mean +- 0.00001)

      df.select(tile_histogram($"rand") as "hist")
        .select($"hist.stats.mean").as[Double]
        .first() should be(mean +- 0.00001)
      df.selectExpr("rf_tile_histogram(rand) as hist")
        .select($"hist.stats.no_data_cells").as[Long]
        .first() should be >= numND.toLong

      checkDocs("rf_tile_histogram")
    }
  }

  describe("aggregate statistics") {
    it("should count data cells") {
      val df = randNDTilesWithNull.toDF("tile")
      df.select(agg_data_cells($"tile")).first() should be (expectedRandData)
      df.selectExpr("rf_agg_data_cells(tile)").as[Long].first() should be (expectedRandData)

      checkDocs("rf_agg_data_cells")
    }
    it("should count no-data cells") {
      val df = randNDTilesWithNull.toDF("tile")
      df.select(agg_no_data_cells($"tile")).first() should be (expectedRandNoData)
      df.selectExpr("rf_agg_no_data_cells(tile)").as[Long].first() should be (expectedRandNoData)
      checkDocs("rf_agg_no_data_cells")
    }

    it("should compute aggregate statistics") {
      val df = randNDTilesWithNull.toDF("tile")

      df
        .select(agg_stats(ExtractTile($"tile")), agg_stats(ExtractTile($"tile"))).printSchema()

      df
        .select(agg_stats($"tile") as "stats")
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
      df.select(agg_histogram($"tile")).show(false)

      checkDocs("rf_agg_histogram")
    }
  }

  describe("analytical transformations") {
    it("should compute normalized_difference") {
      val df = Seq((three, two)).toDF("three", "two")

      df.select(tile_to_array_double(normalized_difference($"three", $"two")))
        .first()
        .forall(_ == 0.2) shouldBe true

      df.selectExpr("rf_tile_to_array_double(rf_normalized_difference(three, two))")
        .as[Array[Double]]
        .first()
        .forall(_ == 0.2) shouldBe true

      checkDocs("rf_normalized_difference")
    }

    it("should mask one tile against another") {
      val df = Seq(randTile).toDF("tile")
      // create an artificial mask for values > 25000; masking value will be 4
      val mask_value = 4

      val rf1 = df.select($"tile",
        local_multiply(convert_cell_type(
          local_greater($"tile", 0.1),
          "uint8"), lit(mask_value)) as "mask")
      val rf2 = rf1.select($"tile",
        mask_by_value($"tile", $"mask", lit(mask_value)) as "masked")

      val result = rf2.agg(agg_no_data_cells($"tile") < agg_no_data_cells($"masked"))

      result.show(false)

      checkDocs("rf_mask")
    }

    it("should render ascii art") {
      val df = Seq[Tile](ProjectedRasterTile(TestData.l8Labels)).toDF("tile")
      val r1 = df.select(render_ascii($"tile"))
      val r2 = df.selectExpr("rf_render_ascii(tile)").as[String]
      r1.first() should be(r2.first())
      checkDocs("rf_render_ascii")
    }

    it("should render cells as matrix") {
      val df = Seq(randDoubleNDTile).toDF("tile")
      val r1 = df.select(render_matrix($"tile"))
      r1.show(false)
      val r2 = df.selectExpr("rf_render_matrix(tile)").as[String]
      r1.first() should be(r2.first())
      checkDocs("rf_render_matrix")
    }

    it("should inverse mask one tile against another") {
      checkDocs("rf_inverse_mask")
      fail("missing test")
    }

    it("should mask tile by onother identified by specified value") {
      checkDocs("rf_mask_by_value")
      fail("missing test")
    }
  }
}