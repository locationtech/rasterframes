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
package astraea.spark.rasterframes.sources

import astraea.spark.rasterframes._
import geotrellis.proj4.LatLng
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.file.{FileLayerReader, FileLayerWriter}
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.tiling.ZoomedLayoutScheme
import geotrellis.vector._


class DataSourceSpec extends TestEnvironment with TestData {

  import sqlContext.implicits._

  lazy val reader = FileLayerReader(outputLocalPath)
  lazy val writer = FileLayerWriter(outputLocalPath)

  val testRdd = {
    val recs: Seq[(SpatialKey, Tile)] = for {
      col <- 2 to 5
      row <- 2 to 5
    } yield SpatialKey(col,row) -> ArrayTile.alloc(DoubleConstantNoDataCellType, 3, 3)

    val rdd = sc.parallelize(recs)
    val scheme = ZoomedLayoutScheme(LatLng, tileSize = 3)
    val layerLayout = scheme.levelForZoom(4).layout
    val layerBounds = KeyBounds(SpatialKey(2,2), SpatialKey(5,5))
    val md = TileLayerMetadata[SpatialKey](
      cellType = DoubleConstantNoDataCellType,
      crs = LatLng,
      bounds = layerBounds,
      layout = layerLayout,
      extent = layerLayout.mapTransform(layerBounds.toGridBounds()))
    ContextRDD(rdd, md)
  }

  // TestEnvironment will clean this up
  writer.write(LayerId("all-ones", 4), testRdd, ZCurveKeyIndexMethod)

  describe("GeoTrellis DataSource") {
    val dfr = sqlContext.read
      .format("geotrellis")
      .option("uri", outputLocal.toUri.toString)
      .option("layer", "all-ones")
      .option("zoom", "4")

    it("should read tiles") {
      val df = dfr.load()
      df.show()
      df.count should be((2 to 5).length * (2 to 5).length)
    }

    it("used produce tile UDT that we can manipulate"){
      val df = dfr.load().select($"col", $"row", $"extent", tileStats($"tile"))
      df.show()
      assert(df.count() > 0)
    }
    it("should respect bbox query"){
      val boundKeys = KeyBounds(SpatialKey(3,4),SpatialKey(4,4))
      val Extent(xmin,ymin,xmax,ymax) = testRdd.metadata.layout.mapTransform(boundKeys.toGridBounds())
      val df = dfr.option("bbox", s"$xmin,$ymin,$xmax,$ymax").load()

      df.count() should be (boundKeys.toGridBounds.sizeLong)
    }

    it("should provide un-packable records"){
      val df = dfr.load().select($"col", $"row", $"extent.xmin", $"tile")
      df.show()
      assert(df.count > 0)
    }

    it("should invoke Encoder[Extent]"){
      val df = dfr.load().select($"col", $"row", $"extent".as[Extent], $"tile")
      df.show()
      assert(df.count > 0)
    }

  }
}
