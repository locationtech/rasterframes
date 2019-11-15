/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Astraea, Inc.
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
import geotrellis.raster.mapalgebra.local.{Add, Max, Min}
import geotrellis.spark._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.locationtech.rasterframes.TestData.randomTile
import org.locationtech.rasterframes.stats.CellHistogram
import org.locationtech.rasterframes.util.DataBiasedOp.BiasedAdd

/**
 * Test rig associated with computing statistics and other descriptive
 * information over tiles.
 *
 * @since 9/18/17
 */
class TileStatsSpec extends TestEnvironment with TestData {
  import TestData.injectND
  import spark.implicits._

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
        val result = Seq(tile).toDF("tile").select(rf_cell_type($"tile")).first()
        result should be(expected)
      }
    }

    // tiles defined for the next few tests
    val tile1 = TestData.fracTile(10, 10, 5)
    val tile2 = ArrayTile(Array(-5, -4, -3, -2, -1, 0, 1, 2, 3), 3, 3)
    val tile3 = randomTile(255, 255, IntCellType)

    it("should compute accurate item counts") {
      val ds = Seq[Tile](tile1, tile2, tile3).toDF("tiles")
      val checkedValues = Seq[Double](0, 4, 7, 13, 26)
      val result = checkedValues.map(x => ds.select(rf_tile_histogram($"tiles")).first().itemCount(x))
      forEvery(checkedValues) { x =>
        assert((x == 0 && result.head == 4) || result.contains(x - 1))
      }
    }

    it("Should compute quantiles") {
      val ds = Seq[Tile](tile1, tile2, tile3).toDF("tiles")
      val numBreaks = 5
      val breaks = ds.select(rf_tile_histogram($"tiles")).map(_.quantileBreaks(numBreaks)).collect()
      assert(breaks(1).length === numBreaks)
      assert(breaks(0).apply(2) == 25)
      assert(breaks(1).max <= 3 && breaks.apply(1).min >= -5)
    }

    it("should support local min/max") {
      import spark.implicits._
      val ds = Seq[Tile](byteArrayTile, byteConstantTile).toDF("tiles")
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

        val ds = Seq.fill[Tile](3)(randomTile(5, 5, FloatConstantNoDataCellType)).toDS()
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
      val ds = Seq.fill[Tile](3)(randomTile(5, 5, FloatCellType)).toDF("tiles")
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
        .fill[Tile](rows)(randomTile(tileSize, tileSize, FloatConstantNoDataCellType))
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
        .map(injectND(2)) :+ null).toDF("tiles")
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

      val ds = (Seq.fill(20)(completeTile) :+ null).toDF("tiles")
      val dsNd = (Seq.fill(20)(completeTile) :+ incompleteTile :+ null).toDF("tiles")

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
      .map(injectND(nds)) :+ null).toDF("tiles")

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
            z ⇒ if(isData(z)) count = count + 1
          ) (
            z ⇒ if(isData(z)) count = count + 1
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
      val df = Seq(lazyPRT, lazyPRT).toDF("tile")

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
