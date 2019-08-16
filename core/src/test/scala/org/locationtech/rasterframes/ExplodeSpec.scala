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
import geotrellis.raster.resample.NearestNeighbor


/**
 * Test rig for Tile operations associated with converting to/from
 * exploded/long form representations of the tile's data.
 *
 * @since 9/18/17
 */
class ExplodeSpec extends TestEnvironment with TestData {
  describe("conversion to/from exploded representation of tiles") {
    import sqlContext.implicits._

    it("should explode tiles") {
      val query = sql(
        """select rf_explode_tiles(
          |  rf_make_constant_tile(1, 10, 10, 'int8raw'),
          |  rf_make_constant_tile(2, 10, 10, 'int8raw')
          |)
          |""".stripMargin)
      write(query)
      assert(query.select("cell_0", "cell_1").as[(Double, Double)].collect().forall(_ == ((1.0, 2.0))))
      val query2 = sql(
        """|select rf_dimensions(tiles) as dims, rf_explode_tiles(tiles) from (
           |select rf_make_constant_tile(1, 10, 10, 'int8raw') as tiles)
           |""".stripMargin)
      write(query2)
      assert(query2.columns.length === 4)

      val df = Seq[(Tile, Tile)]((byteArrayTile, byteArrayTile)).toDF("tile1", "tile2")
      val exploded = df.select(rf_explode_tiles($"tile1", $"tile2"))
      //exploded.printSchema()
      assert(exploded.columns.length === 4)
      assert(exploded.count() === 9)
      write(exploded)
    }

    it("should explode tiles with random sampling") {
      val df = Seq[(Tile, Tile)]((byteArrayTile, byteArrayTile)).toDF("tile1", "tile2")
      val exploded = df.select(rf_explode_tiles_sample(0.5, $"tile1", $"tile2"))
      assert(exploded.columns.length === 4)
      assert(exploded.count() < 9)
    }

    it("should handle null tiles") {
      val df = Seq[Tile](null, byteArrayTile, null, byteArrayTile, null).toDF("tile1")
      val exploded = df.select(rf_explode_tiles($"tile1"))
      assert(exploded.count === byteArrayTile.size * 2)
      val df2 = Seq[(Tile, Tile)]((byteArrayTile, null), (null, byteArrayTile), (byteArrayTile, byteArrayTile)).toDF("tile1", "tile2")
      val exploded2 = df2.select(rf_explode_tiles($"tile1", $"tile2"))
      assert(exploded2.count === byteArrayTile.size * 3)
    }

    it("should handle single tile with user-defined NoData value") {
      // Create a tile with a single (wierd) no-data value
      val tile: Tile = UShortArrayTile(rangeArray(9, _.toShort), 3, 3, 5.toShort)
      val cells = Seq(tile).toDF("tile")
        .select(rf_explode_tiles($"tile"))
        .select($"tile".as[Double])
        .collect()

      cells.count(_.isNaN) should be(1)
    }

    it("should handle user-defined NoData values in tile sampler") {
      val tiles = allTileTypes.filter(t ⇒ !t.isInstanceOf[BitArrayTile]).map(_.withNoData(Some(3)))
      val cells = tiles.toDF("tile")
        .select(rf_explode_tiles($"tile"))
        .select($"tile".as[Double])
        .collect()
      cells.count(_.isNaN) should be(tiles.size)
    }

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

    it("should reassemble single exploded tile") {
      val tile = TestData.randomTile(10, 10, FloatCellType)
      val df = Seq[Tile](tile).toDF("tile")
        .select(rf_explode_tiles($"tile"))

      val assembled = df.agg(
        rf_assemble_tile(
        COLUMN_INDEX_COLUMN,
        ROW_INDEX_COLUMN,
        TILE_COLUMN,
          10, 10, tile.cellType
      )).as[Tile]

      val result = assembled.first()
      assert(result === tile)

      val assembledSqlExpr = df.selectExpr("rf_assemble_tile(column_index, row_index, tile, 10, 10)")

      val resultSql = assembledSqlExpr.as[Tile].first()
      assert(resultSql === tile)

      checkDocs("rf_assemble_tile")
    }

    it("should reassemble single exploded tile with user-defined nodata") {
      val ct = FloatUserDefinedNoDataCellType(-99)
      val tile = TestData.injectND(3)(TestData.randomTile(5, 5, ct))
      val df = Seq[Tile](tile).toDF("tile")
        .select(rf_explode_tiles($"tile"))

      val assembled = df.agg(rf_assemble_tile(
        COLUMN_INDEX_COLUMN,
        ROW_INDEX_COLUMN,
        TILE_COLUMN,
        5, 5, ct
      )).as[Tile]

      val result = assembled.first()
      assert(result === tile)

      // and with SQL API
      logger.info(df.schema.treeString)

      val assembledSqlExpr = df.selectExpr(s"rf_convert_cell_type(rf_assemble_tile(column_index, row_index, tile, 5, 5), '${ct.toString()}') as tile")

      val resultSql = assembledSqlExpr.as[Tile].first()
      assert(resultSql === tile)
      assert(resultSql.cellType === ct)
    }

    it("should reassemble multiple exploded tiles") {
      val image = sampleSmallGeoTiff
      val tinyTiles = image.projectedRaster.toLayer(10, 10)

      val exploded = tinyTiles.select(tinyTiles.spatialKeyColumn, rf_explode_tiles(tinyTiles.tileColumns.head))

      val assembled = exploded.groupBy(tinyTiles.spatialKeyColumn)
        .agg(
          rf_assemble_tile(
          COLUMN_INDEX_COLUMN,
          ROW_INDEX_COLUMN,
          TILE_COLUMN,
          10, 10, IntConstantNoDataCellType
        ))

      val tlm = tinyTiles.tileLayerMetadata.left.get

      val rf = assembled.asLayer(SPATIAL_KEY_COLUMN, tlm)

      val (cols, rows) = image.tile.dimensions

      val recovered = rf.toRaster(TILE_COLUMN, cols, rows, NearestNeighbor)

      //GeoTiff(recovered).write("foo.tiff")

      assert(image.tile.toArrayTile() === recovered.tile.toArrayTile())
    }
  }
}
