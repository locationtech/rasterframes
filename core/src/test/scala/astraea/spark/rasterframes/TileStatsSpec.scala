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
 */

package astraea.spark.rasterframes

import astraea.spark.rasterframes.TestData.randomTile
import astraea.spark.rasterframes.TestData.fracTile
import astraea.spark.rasterframes.stats.CellHistogram
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.raster.mapalgebra.local.{Max, Min}
import org.apache.spark.sql.functions._

/**
 * Test rig associated with computing statistics and other descriptive
 * information over tiles.
 *
 * @since 9/18/17
 */
class TileStatsSpec extends TestEnvironment with TestData {

  import TestData.injectND

  describe("computing statistics over tiles") {
    //import org.apache.spark.sql.execution.debug._
    it("should report dimensions") {
      import sqlContext.implicits._
      val df = Seq[(Tile, Tile)]((byteArrayTile, byteArrayTile)).toDF("tile1", "tile2")

      val dims = df.select(tileDimensions($"tile1") as "dims").select("dims.*")

      assert(dims.as[(Int, Int)].first() === (3, 3))
      assert(dims.schema.head.name === "cols")

      val query = sql(
        """|select dims.* from (
           |select rf_tileDimensions(tiles) as dims from (
           |select rf_makeConstantTile(1, 10, 10, 'int8raw') as tiles))
           |""".stripMargin)
      write(query)
      assert(query.as[(Int, Int)].first() === (10, 10))

      df.repartition(4).createOrReplaceTempView("tmp")
      assert(sql("select dims.* from (select rf_tileDimensions(tile2) as dims from tmp)")
        .as[(Int, Int)].first() === (3, 3))
    }

    // tiles defined for the next few tests
    val tile1 = fracTile(10, 10, 5)
    val tile2 = ArrayTile(Array(-5, -4, -3, -2, -1, 0, 1, 2, 3), 3, 3)
    val tile3 = randomTile(255, 255, IntCellType)

    it("should compute accurate item counts") {
      import sqlContext.implicits._
      val ds = Seq[Tile](tile1, tile2, tile3).toDF("tiles")
      val checkedValues = Seq[Double](0, 4, 7, 13, 26)
      val result = checkedValues.map(x => ds.select(tileHistogram($"tiles")).first().itemCount(x))
      forEvery(checkedValues) { x => assert((x == 0 && result.head == 4) || result.contains(x - 1)) }
    }

    it("Should compute quantiles"){
      import sqlContext.implicits._
      val ds = Seq[Tile](tile1, tile2, tile3).toDF("tiles")
      val numBreaks = 5
      val breaks = ds.select(tileHistogram($"tiles")).map(_.quantileBreaks(numBreaks)).collect()
      assert(breaks(1).length === numBreaks)
      assert(breaks(0).apply(2) == 25)
      assert(breaks(1).max <= 3 && breaks.apply(1).min >= -5)
    }

    it("should support local min/max") {
      import sqlContext.implicits._
      val ds = Seq[Tile](byteArrayTile, byteConstantTile).toDF("tiles")
      ds.createOrReplaceTempView("tmp")

      withClue("max") {
        val max = ds.agg(localAggMax($"tiles"))
        val expected = Max(byteArrayTile, byteConstantTile)
        write(max)
        assert(max.as[Tile].first() === expected)

        val sqlMax = sql("select rf_localAggMax(tiles) from tmp")
        assert(sqlMax.as[Tile].first() === expected)

      }

      withClue("min") {
        val min = ds.agg(localAggMin($"tiles"))
        val expected = Min(byteArrayTile, byteConstantTile)
        write(min)
        assert(min.as[Tile].first() === Min(byteArrayTile, byteConstantTile))

        val sqlMin = sql("select rf_localAggMin(tiles) from tmp")
        assert(sqlMin.as[Tile].first() === expected)
      }
    }

    it("should count data and no-data cells") {
      import sqlContext.implicits._
      val ds = (Seq.fill[Tile](10)(injectND(10)(randomTile(10, 10, UByteConstantNoDataCellType))) :+ null).toDF("tile")
      val expectedNoData = 10 * 10
      val expectedData = 10 * 10 * 10 - expectedNoData

      //logger.debug(ds.select($"tile").as[Tile].first.cellType.name)

      assert(ds.select(dataCells($"tile") as "cells").agg(sum("cells")).as[Long].first() === expectedData)
      assert(ds.select(noDataCells($"tile") as "cells").agg(sum("cells")).as[Long].first() === expectedNoData)

      assert(ds.select(aggDataCells($"tile")).first() === expectedData)
      assert(ds.select(aggNoDataCells($"tile")).first() === expectedNoData)

      val resultTileStats = ds.select(tileStats($"tile")("dataCells") as "cells")
        .agg(sum("cells")).as[Long]
        .first()
      assert(resultTileStats === expectedData)

      val (aggDC, aggNDC) = ds.select(aggStats($"tile")).select("dataCells", "noDataCells").as[(Long, Long)].first()
      assert(aggDC === expectedData)
      assert(aggNDC === expectedNoData)
    }

    it("should compute tile statistics") {
      import sqlContext.implicits._
      withClue("mean") {

        val ds = Seq.fill[Tile](3)(randomTile(5, 5, FloatConstantNoDataCellType)).toDS()
        val means1 = ds.select(tileStats($"value")).map(_.mean).collect
        val means2 = ds.select(tileMean($"value")).collect
        // Compute the mean manually, knowing we're not dealing with no-data values.
        val means = ds.select(tileToArray[Float]($"value")).map(a ⇒ a.sum.toDouble / a.length).collect

        forAll(means.zip(means1)) { case (l, r) ⇒ assert(l === r +- 1e-6) }
        forAll(means.zip(means2)) { case (l, r) ⇒ assert(l === r +- 1e-6) }
      }
      withClue("sum") {
        val rf = l8Sample(1).projectedRaster.toRF
        val expected = 309149454 // computed with rasterio
        val result = rf.agg(sum(tileSum($"tile"))).collect().head.getDouble(0)
        logger.info(s"L8 sample band 1 grand total: ${result}")
        assert(result === expected)
      }
    }

    it("should compute per-tile histogram") {
      import sqlContext.implicits._
      val ds = Seq.fill[Tile](3)(randomTile(5, 5, FloatCellType)).toDF("tiles")
      ds.createOrReplaceTempView("tmp")

      val r1 = ds.select(tileHistogram($"tiles"))
      assert(r1.first.totalCount === 5 * 5)
      write(r1)

      val r2 = sql("select hist.* from (select rf_tileHistogram(tiles) as hist from tmp)").as[CellHistogram]
      write(r2)
      assert(r1.first.mean === r2.first.mean)
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
      import sqlContext.implicits._
      val tileSize = 5
      val rows = 10
      val ds = Seq.fill[Tile](rows)(randomTile(tileSize, tileSize, FloatConstantNoDataCellType)).toDF("tiles")
      ds.createOrReplaceTempView("tmp")
      val agg = ds.select(aggHistogram($"tiles")).as[CellHistogram]
      val histArray = agg.collect()
      assert(histArray.length === 1)

      // examine histogram info
      val hist = histArray.head
      //logger.info(hist.asciiHistogram(128))
      //logger.info(hist.asciiStats)
      assert(hist.totalCount === rows * tileSize * tileSize)
      assert(hist.bins.map(_.count).sum === rows * tileSize * tileSize)

      val stats = agg.map(_.stats).as("stats")
      //stats.select("stats.*").show(false)
      assert(stats.first().stddev === 1.0 +- 0.3) // <-- playing with statistical fire :)

      val hist2 = sql("select hist.* from (select rf_aggHistogram(tiles) as hist from tmp)").as[CellHistogram]

      assert(hist2.first.totalCount === rows * tileSize * tileSize)
    }

    it("should compute aggregate mean") {
      import sqlContext.implicits._
      val ds = (Seq.fill[Tile](10)(randomTile(5, 5, FloatCellType)) :+ null).toDF("tiles")
      val agg = ds.select(aggMean($"tiles"))
      val stats = ds.select(aggStats($"tiles") as "stats").select($"stats.mean".as[Double])
      assert(agg.first() === stats.first())
    }

    it("should compute aggregate statistics") {
      import sqlContext.implicits._
      val ds = Seq.fill[Tile](10)(randomTile(5, 5, FloatConstantNoDataCellType)).toDF("tiles")

      val exploded = ds.select(explodeTiles($"tiles"))
      val (mean, vrnc) = exploded.agg(avg($"tiles"), var_pop($"tiles")).as[(Double, Double)].first

      val stats = ds.select(aggStats($"tiles") as "stats") ///.as[(Long, Double, Double, Double, Double)]

      noException shouldBe thrownBy {
        ds.select(aggStats($"tiles")).collect()
      }

      val agg = stats.select($"stats.variance".as[Double])

      assert(vrnc === agg.first() +- 1e-6)

      ds.createOrReplaceTempView("tmp")
      val agg2 = sql("select stats.* from (select rf_aggStats(tiles) as stats from tmp)")
      assert(agg2.first().getAs[Long]("dataCells") === 250L)

      val agg3 = ds.agg(aggStats($"tiles") as "stats").select($"stats.mean".as[Double])
      assert(mean === agg3.first())
    }

    it("should compute aggregate local stats") {
      import sqlContext.implicits._
      val ave = (nums: Array[Double]) ⇒ nums.sum / nums.length

      val ds = (Seq.fill[Tile](30)(randomTile(5, 5, FloatConstantNoDataCellType))
        .map(injectND(2)) :+ null).toDF("tiles")
      ds.createOrReplaceTempView("tmp")

      val agg = ds.select(localAggStats($"tiles") as "stats")
      val stats = agg.select("stats.*")

      //printStatsRows(stats)

      val min = agg.select($"stats.min".as[Tile]).map(_.toArrayDouble().min).first
      assert(min < -2.0)
      val max = agg.select($"stats.max".as[Tile]).map(_.toArrayDouble().max).first
      assert(max > 2.0)
      val tendancy = agg.select($"stats.mean".as[Tile]).map(t ⇒ ave(t.toArrayDouble())).first
      assert(tendancy < 0.2)

      val varg = agg.select($"stats.mean".as[Tile]).map(t ⇒ ave(t.toArrayDouble())).first
      assert(varg < 1.1)

      val sqlStats = sql("SELECT stats.* from (SELECT rf_localAggStats(tiles) as stats from tmp)")

      val tiles = stats.collect().flatMap(_.toSeq).map(_.asInstanceOf[Tile])
      val dsTiles = sqlStats.collect().flatMap(_.toSeq).map(_.asInstanceOf[Tile])
      forEvery(tiles.zip(dsTiles)) { case (t1, t2) ⇒
        assert(t1 === t2)
      }
    }

    it("should compute accurate statistics") {
      import sqlContext.implicits._
      val completeTile = squareIncrementingTile(4).convert(IntConstantNoDataCellType)
      val incompleteTile = injectND(2)(completeTile)

      val ds = (Seq.fill(20)(completeTile) :+ null).toDF("tiles")
      val dsNd = (Seq.fill(20)(completeTile) :+ incompleteTile :+ null).toDF("tiles")

      // counted everything properly
      val countTile = ds.select(localAggDataCells($"tiles")).first()
      forAll(countTile.toArray())(i ⇒ assert(i === 20))

      val countArray = dsNd.select(localAggDataCells($"tiles")).first().toArray()
      val expectedCount = (completeTile.localDefined().toArray zip incompleteTile.localDefined().toArray())
        .toSeq.map(pr ⇒ pr._1 * 20 + pr._2)
      assert(countArray === expectedCount)

      val countNodataArray = dsNd.select(localAggNoDataCells($"tiles")).first().toArray
      assert(countNodataArray === incompleteTile.localUndefined().toArray)

      // GeoTrellis docs do not say how NODATA is treated, but NODATA values are ignored
      val meanTile = dsNd.select(localAggMean($"tiles")).first()
      assert(meanTile.toArray() === completeTile.toArray())

      // GeoTrellis docs state that Min(1.0, NODATA) = NODATA
      val minTile = dsNd.select(localAggMin($"tiles")).first()
      assert(minTile.toArray() === incompleteTile.toArray())

      // GeoTrellis docs state that Max(1.0, NODATA) = NODATA
      val maxTile = dsNd.select(localAggMax($"tiles")).first()
      assert(maxTile.toArray() === incompleteTile.toArray())
    }

    it("should count cells by no-data state") {
      import sqlContext.implicits._
      val tsize = 5
      val count = 20
      val nds = 2
      val tiles = (Seq.fill[Tile](count)(randomTile(tsize, tsize, UByteUserDefinedNoDataCellType(255.toByte)))
        .map(injectND(nds)) :+ null).toDF("tiles")

      val counts = tiles.select(noDataCells($"tiles")).collect().dropRight(1)
      forEvery(counts)(c ⇒ assert(c === nds))
      val counts2 = tiles.select(dataCells($"tiles")).collect().dropRight(1)
      forEvery(counts2)(c ⇒ assert(c === tsize * tsize - nds))
    }
  }
}
