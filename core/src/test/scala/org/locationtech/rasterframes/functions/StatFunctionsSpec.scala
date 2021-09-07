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

import geotrellis.raster._
import geotrellis.raster.mapalgebra.local._
import geotrellis.spark._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.locationtech.rasterframes.TestData._
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.stats._
import org.locationtech.rasterframes.util.DataBiasedOp._

class StatFunctionsSpec extends TestEnvironment with TestData {
  import spark.implicits._

  val df = TestData.sampleGeoTiff
    .toDF()
    .withColumn("tilePlus2", rf_local_add(col("tile"), 2))


  describe("Tile quantiles through built-in functions") {

    it("should compute approx percentiles for a single tile col") {
      // Use "explode"
      val result = df
        .select(rf_explode_tiles($"tile"))
        .stat
        .approxQuantile("tile", Array(0.10, 0.50, 0.90), 0.00001)

      result.length should be(3)

      // computing externally with numpy we arrive at 7963, 10068, 12160 for these quantiles
      result should contain inOrderOnly(7963.0, 10068.0, 12160.0)

      // Use "to_array" and built-in explode
      val result2 = df
        .select(explode(rf_tile_to_array_double($"tile")) as "tile")
        .stat
        .approxQuantile("tile", Array(0.10, 0.50, 0.90), 0.00001)

      result2.length should be(3)

      // computing externally with numpy we arrive at 7963, 10068, 12160 for these quantiles
      result2 should contain inOrderOnly(7963.0, 10068.0, 12160.0)
    }
  }

  describe("Tile quantiles through custom aggregate") {
    it("should compute approx percentiles for a single tile col") {
      val result = df
        .select(rf_agg_approx_quantiles($"tile", Seq(0.1, 0.5, 0.9)))
        .first()

      result.length should be(3)

      // computing externally with numpy we arrive at 7963, 10068, 12160 for these quantiles
      result should contain inOrderOnly(7963.0, 10068.0, 12160.0)
    }
  }

  describe("per-tile stats") {
    it("should compute data cell counts") {
      val df = Seq(Option(TestData.injectND(numND)(two))).toDF("two")
      df.select(rf_data_cells($"two")).first() shouldBe (cols * rows - numND).toLong

      val df2 = randNDTilesWithNullOptional.toDF("tile")
      df2
        .select(rf_data_cells($"tile") as "cells")
        .agg(sum("cells"))
        .as[Long]
        .first() should be(expectedRandData)

      checkDocs("rf_data_cells")
    }
    it("should compute no-data cell counts") {
      val df = Seq(Option(TestData.injectND(numND)(two))).toDF("two")
      df.select(rf_no_data_cells($"two")).first() should be(numND)

      val df2 = randNDTilesWithNullOptional.toDF("tile")
      df2
        .select(rf_no_data_cells($"tile") as "cells")
        .agg(sum("cells"))
        .as[Long]
        .first() should be(expectedRandNoData)

      checkDocs("rf_no_data_cells")
    }

    it("should properly count data and nodata cells on constant tiles") {
      val rf = Seq(Option(randPRT)).toDF("tile")

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

      counts should be((0L, 12L, 12L, 0L))
    }

    it("should detect no-data tiles") {
      val df = Seq(Option(nd)).toDF("nd")
      df.select(rf_is_no_data_tile($"nd")).first() should be(true)
      val df2 = Seq(Option(two)).toDF("not_nd")
      df2.select(rf_is_no_data_tile($"not_nd")).first() should be(false)
      checkDocs("rf_is_no_data_tile")
    }

    it("should evaluate exists and for_all") {
      val df0 = Seq(Option(zero)).toDF("tile")
      df0.select(rf_exists($"tile")).first() should be(false)
      df0.select(rf_for_all($"tile")).first() should be(false)

      Seq(Option(one)).toDF("tile").select(rf_exists($"tile")).first() should be(true)
      Seq(Option(one)).toDF("tile").select(rf_for_all($"tile")).first() should be(true)

      val dfNd = Seq(Option(TestData.injectND(1)(one))).toDF("tile")
      dfNd.select(rf_exists($"tile")).first() should be(true)
      dfNd.select(rf_for_all($"tile")).first() should be(false)

      checkDocs("rf_exists")
      checkDocs("rf_for_all")
    }

    it("should check values is_in") {
      checkDocs("rf_local_is_in")

      // tile is 3 by 3 with values, 1 to 9
      val rf = Seq(Option(byteArrayTile)).toDF("t")
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
      val df = Seq(Option(randNDPRT)).toDF("rand")
      df.select(rf_tile_mean($"rand")).first() should be(mean)
      df.selectExpr("rf_tile_mean(rand)").as[Double].first() should be(mean)
      checkDocs("rf_tile_mean")
    }

    it("should compute the tile summary statistics") {
      val values = randNDPRT.toArray().filter(c => isData(c))
      val mean = values.sum.toDouble / values.length
      val df = Seq(Option(randNDPRT)).toDF("rand")
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

      val df2 = randNDTilesWithNullOptional.toDF("tile")
      df2
        .select(rf_tile_stats($"tile")("data_cells") as "cells")
        .agg(sum("cells"))
        .as[Long]
        .first() should be(expectedRandData)

      checkDocs("rf_tile_stats")
    }

    it("should compute the tile histogram") {
      val df = Seq(Option(randNDPRT)).toDF("rand")
      val h1 = df.select(rf_tile_histogram($"rand")).first()

      val h2 = df
        .selectExpr("rf_tile_histogram(rand) as hist")
        .select($"hist".as[CellHistogram])
        .first()

      h1 should be(h2)

      checkDocs("rf_tile_histogram")
    }
  }

  describe("computing statistics over tiles") {
    //import org.apache.spark.sql.execution.debug._
    it("should report dimensions") {
      val df = Seq[(Tile, Tile)]((byteArrayTile, byteArrayTile)).toDF("tile1", "tile2")

      val dims = df.select(rf_dimensions($"tile1") as "dims").select("dims.*")

      assert(dims.as[(Int, Int)].first() === (3, 3))
      assert(dims.schema.head.name === "cols")

      val query = sql(
        """|select dims.* from (
           |select rf_dimensions(tiles) as dims from (
           |select rf_make_constant_tile(1, 10, 10, 'int8raw') as tiles))
           |""".stripMargin)
      write(query)
      assert(query.as[(Int, Int)].first() === (10, 10))

      df.repartition(4).createOrReplaceTempView("tmp")
      assert(
        sql("select dims.* from (select rf_dimensions(tile2) as dims from tmp)")
          .as[(Int, Int)]
          .first() === (3, 3))
    }

    it("should report cell type") {
      val ct = functions.cellTypes().filter(_ != "bool")
      forEvery(ct) { c =>
        val expected = CellType.fromName(c)
        val tile = randomTile(5, 5, expected)
        val result = Seq(Option(tile)).toDF("tile").select(rf_cell_type($"tile")).first()
        result should be(expected)
      }
    }

    // tiles defined for the next few tests
    val tile1 = TestData.fracTile(10, 10, 5)
    val tile2 = ArrayTile(Array(-5, -4, -3, -2, -1, 0, 1, 2, 3), 3, 3)
    val tile3 = randomTile(255, 255, IntCellType)

    it("should compute accurate item counts") {
      val ds = Seq[Option[Tile]](Option(tile1), Option(tile2), Option(tile3)).toDF("tiles")
      val checkedValues = Seq[Double](0, 4, 7, 13, 26)
      val result = checkedValues.map(x => ds.select(rf_tile_histogram($"tiles")).first().itemCount(x))
      forEvery(checkedValues) { x =>
        assert((x == 0 && result.head == 4) || result.contains(x - 1))
      }
    }

    it("Should compute quantiles") {
      val ds = Seq[Option[Tile]](Option(tile1), Option(tile2), Option(tile3)).toDF("tiles")
      val numBreaks = 5
      val breaks = ds.select(rf_tile_histogram($"tiles")).map(_.quantileBreaks(numBreaks)).collect()
      assert(breaks(1).length === numBreaks)
      assert(breaks(0).apply(2) == 25)
      assert(breaks(1).max <= 3 && breaks.apply(1).min >= -5)
    }

    it("should support local min/max") {
      import spark.implicits._
      val ds = Seq[Option[Tile]](Option(byteArrayTile), Option(byteConstantTile)).toDF("tiles")
      ds.createOrReplaceTempView("tmp")

      withClue("max") {
        val max = ds.agg(rf_agg_local_max($"tiles"))
        val expected = Max(byteArrayTile, byteConstantTile)
        write(max)
        assert(max.as[Tile].first() === expected)

        val sqlMax = sql("select rf_agg_local_max(tiles) from tmp")
        assert(sqlMax.as[Tile].first() === expected)
      }

      withClue("min") {
        val min = ds.agg(rf_agg_local_min($"tiles"))
        val expected = Min(byteArrayTile, byteConstantTile)
        write(min)
        assert(min.as[Tile].first() === Min(byteArrayTile, byteConstantTile))

        val sqlMin = sql("select rf_agg_local_min(tiles) from tmp")
        assert(sqlMin.as[Tile].first() === expected)
      }
    }

    it("should compute tile statistics") {
      import spark.implicits._
      withClue("mean") {

        val ds = Seq.fill[Tile](3)(randomTile(5, 5, FloatConstantNoDataCellType)).map(Option(_)).toDS()
        val means1 = ds.select(rf_tile_stats($"value")).map(_.mean).collect
        val means2 = ds.select(rf_tile_mean($"value")).collect
        // Compute the mean manually, knowing we're not dealing with no-data values.
        val means =
          ds.select(rf_tile_to_array_double($"value")).map(a => a.sum / a.length).collect

        forAll(means.zip(means1)) { case (l, r) => assert(l === r +- 1e-6) }
        forAll(means.zip(means2)) { case (l, r) => assert(l === r +- 1e-6) }
      }
      withClue("sum") {
        val rf = l8Sample(1).toDF()
        val expected = 309149454 // computed with rasterio
        val result = rf.agg(sum(rf_tile_sum($"tile"))).collect().head.getDouble(0)
        logger.info(s"L8 sample band 1 grand total: ${result}")
        assert(result === expected)
      }
    }

    it("should compute per-tile histogram") {
      val ds = Seq.fill[Option[Tile]](3)(Option(randomTile(5, 5, FloatCellType))).toDF("tiles")
      ds.createOrReplaceTempView("tmp")

      val r1 = ds.select(rf_tile_histogram($"tiles"))
      assert(r1.first.totalCount === 5 * 5)
      write(r1)
      val r2 = sql("select hist.* from (select rf_tile_histogram(tiles) as hist from tmp)").as[CellHistogram]
      write(r2)
      assert(r1.first === r2.first)
    }

    it("should compute mean and total count") {
      val tileSize = 5

      def rndTile = {
        val data = Array.fill(tileSize * tileSize)(scala.util.Random.nextGaussian())
        ArrayTile(data, tileSize, tileSize): Tile
      }

      val rdd = spark.sparkContext.makeRDD(Seq((1, rndTile), (2, rndTile), (3, rndTile)))
      val h = rdd.histogram()

      assert(h.totalCount() == math.pow(tileSize, 2) * 3)
      assert(math.abs(h.mean().getOrElse((-100).toDouble)) < 3)
    }

    it("should compute aggregate histogram") {
      val tileSize = 5
      val rows = 10
      val ds = Seq
        .fill[Option[Tile]](rows)(Option(randomTile(tileSize, tileSize, FloatConstantNoDataCellType)))
        .toDF("tiles")
      ds.createOrReplaceTempView("tmp")
      val agg = ds.select(rf_agg_approx_histogram($"tiles"))

      val histArray = agg.collect()
      histArray.length should be (1)

      // examine histogram info
      val hist = histArray.head
      assert(hist.totalCount === rows * tileSize * tileSize)
      assert(hist.bins.map(_.count).sum === rows * tileSize * tileSize)

      val hist2 = sql("select hist.* from (select rf_agg_approx_histogram(tiles) as hist from tmp)").as[CellHistogram]

      hist2.first.totalCount should be (rows * tileSize * tileSize)

      checkDocs("rf_agg_approx_histogram")
    }

    it("should compute aggregate mean") {
      val ds = (Seq.fill[Tile](10)(randomTile(5, 5, FloatCellType)) :+ null).toDF("tiles")
      val agg = ds.select(rf_agg_mean($"tiles"))
      val stats = ds.select(rf_agg_stats($"tiles") as "stats").select($"stats.mean".as[Double])
      assert(agg.first() === stats.first())
    }

    it("should compute aggregate statistics") {
      val ds = Seq.fill[Tile](10)(randomTile(5, 5, FloatConstantNoDataCellType)).toDF("tiles")

      val exploded = ds.select(rf_explode_tiles($"tiles"))
      val (mean, vrnc) = exploded.agg(avg($"tiles"), var_pop($"tiles")).as[(Double, Double)].first

      val stats = ds.select(rf_agg_stats($"tiles") as "stats") ///.as[(Long, Double, Double, Double, Double)]
      //stats.printSchema()
      noException shouldBe thrownBy {
        ds.select(rf_agg_stats($"tiles")).collect()
      }

      val agg = stats.select($"stats.variance".as[Double])

      assert(vrnc === agg.first() +- 1e-6)

      ds.createOrReplaceTempView("tmp")
      val agg2 = sql("select stats.* from (select rf_agg_stats(tiles) as stats from tmp)")
      assert(agg2.first().getAs[Long]("data_cells") === 250L)

      val agg3 = ds.agg(rf_agg_stats($"tiles") as "stats").select($"stats.mean".as[Double])
      assert(mean === agg3.first())
    }

    it("should compute aggregate local stats") {
      import spark.implicits._
      val ave = (nums: Array[Double]) => nums.sum / nums.length

      val ds = (Seq
        .fill[Tile](30)(randomTile(5, 5, FloatConstantNoDataCellType))
        .map(injectND(2)) :+ null)
        .map(Option.apply)
        .toDF("tiles")
      ds.createOrReplaceTempView("tmp")

      val agg = ds.select(rf_agg_local_stats($"tiles") as "stats")
      val stats = agg.select("stats.*")

      //printStatsRows(stats)

      val min = agg.select($"stats.min".as[Tile]).map(_.toArrayDouble().min).first
      assert(min < -2.0)
      val max = agg.select($"stats.max".as[Tile]).map(_.toArrayDouble().max).first
      assert(max > 2.0)
      val tendancy = agg.select($"stats.mean".as[Tile]).map(t => ave(t.toArrayDouble())).first
      assert(tendancy < 0.2)

      val varg = agg.select($"stats.mean".as[Tile]).map(t => ave(t.toArrayDouble())).first
      assert(varg < 1.1)

      val sqlStats = sql("SELECT stats.* from (SELECT rf_agg_local_stats(tiles) as stats from tmp)")

      val tiles = stats.collect().flatMap(_.toSeq).map(_.asInstanceOf[Tile])
      val dsTiles = sqlStats.collect().flatMap(_.toSeq).map(_.asInstanceOf[Tile])
      forEvery(tiles.zip(dsTiles)) {
        case (t1, t2) =>
          assert(t1 === t2)
      }
    }

    it("should compute accurate statistics") {
      val completeTile = squareIncrementingTile(4).convert(IntConstantNoDataCellType)
      val incompleteTile = injectND(2)(completeTile)

      val ds = (Seq.fill(20)(completeTile).map(Option(_)) :+ null).toDF("tiles")
      val dsNd = (Seq.fill(20)(completeTile) :+ incompleteTile :+ null).map(Option.apply).toDF("tiles")

      // counted everything properly
      val countTile = ds.select(rf_agg_local_data_cells($"tiles")).first()
      forAll(countTile.toArray())(i => assert(i === 20))

      val countArray = dsNd.select(rf_agg_local_data_cells($"tiles")).first().toArray()
      val expectedCount =
        (completeTile.localDefined().toArray zip incompleteTile.localDefined().toArray()).toSeq.map(
          pr => pr._1 * 20 + pr._2)
      assert(countArray === expectedCount)

      val countNodataArray = dsNd.select(rf_agg_local_no_data_cells($"tiles")).first().toArray
      assert(countNodataArray === incompleteTile.localUndefined().toArray)

      //      val meanTile = dsNd.select(rf_agg_local_mean($"tiles")).first()
      //      assert(meanTile.toArray() === completeTile.toArray())

      val maxTile = dsNd.select(rf_agg_local_max($"tiles")).first()
      assert(maxTile.toArray() === completeTile.toArray())

      val minTile = dsNd.select(rf_agg_local_min($"tiles")).first()
      assert(minTile.toArray() === completeTile.toArray())
    }
  }
  describe("NoData handling") {
    val tsize = 5
    val count = 20
    val nds = 2
    val tiles = (Seq
      .fill[Tile](count)(randomTile(tsize, tsize, UByteUserDefinedNoDataCellType(255.toByte)))
      .map(injectND(nds)) :+ null)
      .map(Option.apply)
      .toDF("tiles")

    it("should count cells by NoData state") {
      val counts = tiles.select(rf_no_data_cells($"tiles")).collect().dropRight(1)
      forEvery(counts)(c => assert(c === nds))
      val counts2 = tiles.select(rf_data_cells($"tiles")).collect().dropRight(1)
      forEvery(counts2)(c => assert(c === tsize * tsize - nds))
    }

    it("should detect all NoData tiles") {
      val ndCount = tiles.select("*").where(rf_is_no_data_tile($"tiles")).count()
      ndCount should be(1)

      val ndTiles =
        (Seq.fill[Tile](count)(ArrayTile.empty(UByteConstantNoDataCellType, tsize, tsize)) :+ null)
          .map(Option.apply)
          .toDF("tiles")
      val ndCount2 = ndTiles.select("*").where(rf_is_no_data_tile($"tiles")).count()
      ndCount2 should be(count + 1)
    }

    // Awaiting https://github.com/locationtech/geotrellis/issues/3153 to be fixed and integrated
    ignore("should allow NoData algebra to be changed via delegating tile") {
      val t1 = ArrayTile(Array.fill(4)(1), 2, 2)
      val t2 = {
        val d = Array.fill(4)(2)
        d(1) = geotrellis.raster.NODATA
        ArrayTile(d, 2, 2)
      }

      val d1 = new DelegatingTile {
        override def delegate: Tile = t1
      }
      val d2 = new DelegatingTile {
        override def delegate: Tile = t2
      }

      /** Counts the number of non-NoData cells in a tile */
      case object CountData {
        def apply(t: Tile) = {
          var count: Long = 0
          t.dualForeach(
            z => if(isData(z)) count = count + 1
          ) (
            z => if(isData(z)) count = count + 1
          )
          count
        }
      }

      // Confirm counts
      CountData(t1) should be (4L)
      CountData(t2) should be (3L)
      CountData(d1) should be (4L)
      CountData(d2) should be (3L)

      // Standard Add evaluates `x + NoData` as `NoData`
      CountData(Add(t1, t2)) should be (3L)
      CountData(Add(d1, d2)) should be (3L)
      // Is commutative
      CountData(Add(t2, t1)) should be (3L)
      CountData(Add(d2, d1)) should be (3L)

      // With BiasedAdd, all cells should be data cells
      CountData(BiasedAdd(t1, t2)) should be (4L) // <-- passes
      CountData(BiasedAdd(d1, d2)) should be (4L) // <-- fails
      // Should be commutative.
      CountData(BiasedAdd(t2, t1)) should be (4L) // <-- passes
      CountData(BiasedAdd(d2, d1)) should be (4L) // <-- fails
    }
  }

  describe("proj_raster handling") {
    it("should handle proj_raster structures") {
      val df = Seq(lazyPRT, lazyPRT).map(Option(_)).toDF("tile")

      val targets = Seq[Column => Column](
        rf_is_no_data_tile,
        rf_data_cells,
        rf_no_data_cells,
        rf_agg_local_max,
        rf_agg_local_min,
        rf_agg_local_mean,
        rf_agg_local_data_cells,
        rf_agg_local_no_data_cells,
        rf_agg_local_stats,
        rf_agg_approx_histogram,
        rf_tile_histogram,
        rf_tile_stats,
        rf_tile_mean,
        rf_tile_max,
        rf_tile_min
      )

      forEvery(targets) { f =>
        noException shouldBe thrownBy {
          df.select(f($"tile")).collect()
        }
      }
    }
  }
}
