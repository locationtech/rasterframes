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

import geotrellis.raster._
import geotrellis.raster.mapalgebra.local._
import org.apache.spark.sql.functions._
import astraea.spark.rasterframes.util._

/**
 * Test rig for Spark UDTs and friends for GT.
 * Extra debugging can be enabled by adding this
 * {{{
 *   import org.apache.spark.sql.execution.debug._
 * }}}
 *
 * @since 3/30/17
 */
class GTSQLSpec extends TestEnvironment with TestData  {
  import TestData.{makeTiles, randomTile}
  import sqlContext.implicits._

  sqlContext.udf.register("rf_makeTiles", makeTiles)

  describe("Dataframe Ops on GeoTrellis types") {
    it("should resolve column names") {
      // This tests an internal utility.
      assert(col("fred").columnName === "fred")
      assert(col("fred").as("barney").columnName === "barney")
    }

    it("should create constant tiles") {
      val query = sql("select rf_makeConstantTile(1, 10, 10, 'int8raw')")
      write(query)
      val tile = query.as[Tile].first
      assert(tile.cellType.equalDataType(ByteCellType))
    }

    it("should generate multiple rows") {
      val query = sql("select explode(rf_makeTiles(3))")
      write(query)
      assert(query.count === 3)
    }

    it("should extract cell types") {
      val expected = allTileTypes.map(_.cellType).toSet
      val df = (allTileTypes :+ null).toDF("tile")
      val types = df.select(cellType($"tile"))

      df.repartition(4).createOrReplaceTempView("tmp")
      sql("select rf_cellType(tile) from tmp").show

      val typeValues = types.collect().filter(_ != null).map(CellType.fromName).toSet
      assert(typeValues === expected)

      intercept[org.apache.spark.sql.AnalysisException] {
        val notTiles = Seq("one", "two", "three").toDF("not_tiles")
        notTiles.select(cellType($"not_tiles")).collect
      }
    }

    it("should list supported cell types") {
      import astraea.spark.rasterframes.functions.cellTypes
      val ct = sql("select explode(rf_cellTypes())").as[String].collect
      forEvery(cellTypes()) { c ⇒
        assert(ct.contains(c))
      }
    }

    it("should support local algebra") {
      val ds = Seq[(Tile, Tile)]((byteArrayTile, byteConstantTile)).toDF("left", "right")
      ds.createOrReplaceTempView("tmp")

      withClue("add") {
        val sum = ds.select(localAdd($"left", $"right"))
        val expected = Add(byteArrayTile, byteConstantTile)
        assert(sum.as[Tile].first() === expected)

        val sqlSum = sql("select rf_localAdd(left, right) from tmp")
        assert(sqlSum.as[Tile].first() === expected)
      }

      withClue("subtract") {
        val sub = ds.select(localSubtract($"left", $"right")).as[Tile].first()
        // remove toArrayTile when https://github.com/locationtech/geotrellis/issues/2493 is released
        val expected = Subtract(byteArrayTile, byteConstantTile.toArrayTile())
        assert(sub === expected)

        val sqlSub = sql("select rf_localSubtract(left, right) from tmp")
        assert(sqlSub.as[Tile].first() === expected)
      }

      withClue("multiply") {
        val sub = ds.select(localMultiply($"left", $"right"))
        val expected = Multiply(byteArrayTile, byteConstantTile)
        assert(sub.as[Tile].first() === expected)

        val sqlSub = sql("select rf_localMultiply(left, right) from tmp")
        assert(sqlSub.as[Tile].first() === expected)
      }

      withClue("divide") {
        val sub = ds.select(localDivide($"left", $"right"))
        // remove toArrayTile when https://github.com/locationtech/geotrellis/issues/2493 is released
        val expected = Divide(byteArrayTile, byteConstantTile.toArrayTile())
        assert(sub.as[Tile].first() === expected)

        val sqlSub = sql("select rf_localDivide(left, right) from tmp")
        assert(sqlSub.as[Tile].first() === expected)
      }
    }

    it("aggregate functions should handle null tiles") {
      val aggs = Seq(localAggMax _, localAggMin _, localAggDataCells _)

      val datasets = Seq(
        {
          val tiles = Array.fill[Tile](30)(randomTile(5, 5, "float32"))
          tiles(1) = null
          tiles(11) = null
          tiles(29) = null
          tiles.toSeq
        },
        Seq.fill[Tile](30)(null)
      )

      forEvery(datasets) { tiles ⇒
        val ds = tiles.toDF("tiles")
        val agg = ds.select(localAggStats($"tiles") as "stats")
        val stats = agg.select("stats.*")
        val statTiles = stats.collect().flatMap(_.toSeq).map(_.asInstanceOf[Tile])
        assert(statTiles.length === 5)

        forEvery(aggs) { aggregate ⇒
          assert(ds.select(aggregate($"tiles")).count() === 1)
        }
      }
    }
  }

  protected def withFixture(test: Any) = ???
}
