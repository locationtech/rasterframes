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
        """select rf_explodeTiles(
          |  rf_makeConstantTile(1, 10, 10, 'int8raw'),
          |  rf_makeConstantTile(2, 10, 10, 'int8raw')
          |)
          |""".stripMargin)
      write(query)
      assert(query.select("cell_0", "cell_1").as[(Double, Double)].collect().forall(_ == ((1.0, 2.0))))
      val query2 = sql(
        """|select rf_tileDimensions(tiles) as dims, rf_explodeTiles(tiles) from (
           |select rf_makeConstantTile(1, 10, 10, 'int8raw') as tiles)
           |""".stripMargin)
      write(query2)
      assert(query2.columns.length === 4)

      val df = Seq[(Tile, Tile)]((byteArrayTile, byteArrayTile)).toDF("tile1", "tile2")
      val exploded = df.select(explodeTiles($"tile1", $"tile2"))
      //exploded.printSchema()
      assert(exploded.columns.length === 4)
      assert(exploded.count() === 9)
      write(exploded)
    }

    it("should explode tiles with random sampling") {
      val df = Seq[(Tile, Tile)]((byteArrayTile, byteArrayTile)).toDF("tile1", "tile2")
      val exploded = df.select(explodeTilesSample(0.5, $"tile1", $"tile2"))
      assert(exploded.columns.length === 4)
      assert(exploded.count() < 9)
    }

    it("should handle null tiles") {
      val df = Seq[Tile](null, byteArrayTile, null, byteArrayTile, null).toDF("tile1")
      val exploded = df.select(explodeTiles($"tile1"))
      assert(exploded.count === byteArrayTile.size * 2)
      val df2 = Seq[(Tile, Tile)]((byteArrayTile, null), (null, byteArrayTile), (byteArrayTile, byteArrayTile)).toDF("tile1", "tile2")
      val exploded2 = df2.select(explodeTiles($"tile1", $"tile2"))
      assert(exploded2.count === byteArrayTile.size * 3)
    }

    it("should handle single tile with user-defined NoData value") {
      // Create a tile with a single (wierd) no-data value
      val tile: Tile = UShortArrayTile(rangeArray(9, _.toShort), 3, 3, 5.toShort)
      val cells = Seq(tile).toDF("tile")
        .select(explodeTiles($"tile"))
        .select($"tile".as[Double])
        .collect()

      assert(cells.count(_.isNaN) === 1)
    }

    it("should handle user-defined NoData values in tile sampler") {
      val tiles = allTileTypes.filter(t ⇒ !t.isInstanceOf[BitArrayTile]).map(_.withNoData(Some(3)))
      val cells = tiles.toDF("tile")
        .select(explodeTiles($"tile"))
        .select($"tile".as[Double])
        .collect()
      assert(cells.count(_.isNaN) === tiles.size)
    }

    it("should convert tile into array") {
      val query = sql(
        """select rf_tileToArrayInt(
          |  rf_makeConstantTile(1, 10, 10, 'int8raw')
          |) as intArray
          |""".stripMargin)
      assert(query.as[Array[Int]].first.sum === 100)

      val tile = FloatConstantTile(1.1f, 10, 10, FloatCellType)
      val df = Seq[Tile](tile).toDF("tile")
      val arrayDF = df.select(tileToArray[Float]($"tile").as[Array[Float]])
      assert(arrayDF.first().sum === 110.0f +- 0.0001f)
    }

    it("should convert an array into a tile") {
      val tile = FloatConstantTile(1.1f, 10, 10, FloatCellType)
      val df = Seq[Tile](tile, null).toDF("tile")
      val arrayDF = df.withColumn("tileArray", tileToArray[Float]($"tile"))

      val back = arrayDF.withColumn("backToTile", arrayToTile($"tileArray", 10, 10))

      val result = back.select($"backToTile".as[Tile]).first

      assert(result.toArrayDouble() === tile.toArrayDouble())

      val hasNoData = back.withColumn("withNoData", withNoData($"backToTile", 0))

      val result2 = hasNoData.select($"withNoData".as[Tile]).first

      assert(result2.cellType.asInstanceOf[UserDefinedNoData[_]].noDataValue === 0)
    }

    it("should reassemble single exploded tile") {
      val df = Seq[Tile](byteArrayTile).toDF("tile")
        .select(explodeTiles($"tile"))

      val assembled = df.agg(assembleTile(
        COLUMN_INDEX_COLUMN,
        ROW_INDEX_COLUMN,
        TILE_COLUMN,
        3, 3, byteArrayTile.cellType
      )).as[Tile]

      val result = assembled.first()
      assert(result === byteArrayTile)
    }

    it("should reassemble multiple exploded tiles") {
      val image = sampleSmallGeoTiff
      val tinyTiles = image.projectedRaster.toRF(10, 10)

      val exploded = tinyTiles.select(tinyTiles.spatialKeyColumn, explodeTiles(tinyTiles.tileColumns.head))

      //exploded.printSchema()

      val assembled = exploded.groupBy(tinyTiles.spatialKeyColumn)
        .agg(assembleTile(
          COLUMN_INDEX_COLUMN,
          ROW_INDEX_COLUMN,
          TILE_COLUMN,
          10, 10, IntConstantNoDataCellType
        ))

      val tlm = tinyTiles.tileLayerMetadata.left.get

      val rf = assembled.asRF(SPATIAL_KEY_COLUMN, tlm)

      val (cols, rows) = image.tile.dimensions

      val recovered = rf.toRaster(TILE_COLUMN, cols, rows, NearestNeighbor)

      //GeoTiff(recovered).write("foo.tiff")

      assert(image.tile.toArrayTile() === recovered.tile.toArrayTile())
    }
  }
}
