/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea, Inc.
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

package astraea.spark.rasterframes.ref

import astraea.spark.rasterframes._
import astraea.spark.rasterframes.expressions._
import astraea.spark.rasterframes.ref.RasterRefSpec.ReadMonitor
import astraea.spark.rasterframes.ref.RasterSource.ReadCallback
import astraea.spark.rasterframes.tiles.ProjectedRasterTile
import astraea.spark.rasterframes.tiles.ProjectedRasterTile.SourceKind
import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.LatLng
import geotrellis.raster.{ByteConstantNoDataCellType, Tile, TileLayout, UByteConstantNoDataCellType}
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.vector.Extent

/**
 *
 *
 * @since 8/22/18
 */
//noinspection TypeAnnotation
class RasterRefSpec extends TestEnvironment with TestData {
  def sub(e: Extent) = {
    val c = e.center
    val w = e.width
    val h = e.height
    Extent(c.x, c.y, c.x + w * 0.01, c.y + h * 0.01)
  }

  trait Fixture {
    val counter = new ReadMonitor
    val src = RasterSource(remoteCOGSingleband1, Some(counter))
    val fullRaster = RasterRef(src)
    val subExtent = sub(src.extent)
    val subRaster = RasterRef(src, subExtent)
  }

  describe("GetCRS Expression") {
    it("should read from RasterRef") {
      import spark.implicits._
      new Fixture {
        val ds = Seq(fullRaster).toDF("ref")
        val crs = ds.select(GetCRS($"ref"))
        assert(crs.count() === 1)
        assert(crs.first() !== null)
      }
    }
    it("should read from resolved RasterRef") {
      import spark.implicits._
      new Fixture {
        val ds = Seq(subRaster).toDF("ref")
        val crs = ds.select(GetCRS(TileWrapRasterRef($"ref")))
        assert(crs.count() === 1)
        assert(crs.first() !== null)
      }
    }
  }

  describe("GetExtent Expression") {
    it("should read from RasterRef") {
      import spark.implicits._
      new Fixture {
        val ds = Seq(fullRaster).toDF("ref")
        val crs = ds.select(GetExtent($"ref"))
        assert(crs.count() === 1)
        assert(crs.first() !== null)
      }
    }
    it("should read from resolved RasterRef") {
      import spark.implicits._
      new Fixture {
        val ds = Seq(subRaster).toDF("ref")
        val crs = ds.select(GetExtent(TileWrapRasterRef($"ref")))
        assert(crs.count() === 1)
        assert(crs.first() !== null)
      }
    }
  }

  describe("RasterRef") {
    it("should delay reading") {
      new Fixture {
        assert(subRaster.cellType === src.cellType)
        assert(counter.reads === 0)
      }
    }
    it("should support subextents") {
      new Fixture {
        assert(subRaster.cols.toDouble === src.cols * 0.01 +- 2.0)
        assert(subRaster.rows.toDouble === src.rows * 0.01 +- 2.0)
        assert(counter.reads === 0)
        //subRaster.tile.rescale(0, 255).renderPng().write("target/foo1.png")
      }
    }
    it("should be realizable") {
      new Fixture {
        assert(counter.reads === 0)
        assert(subRaster.tile.statistics.map(_.dataCells) === Some(subRaster.cols * subRaster.rows))
        assert(counter.reads > 0)
        println(counter)
      }
    }
//    it("should be reprojectable") {
//      new Fixture {
//        val reprojected = subRaster.tile.reproject(LatLng)
//        assert(counter.reads === 0)
//        assert(reprojected.dimensions === (subRaster.cols, subRaster.rows))
//        assert(counter.reads > 0)
//      }
//    }

    it("should be Dataset compatible") {
      import spark.implicits._
      new Fixture {
        val ds = Seq(subRaster).toDS()
        assert(ds.first().isInstanceOf[RasterRef])
        val ds2 = ds.withColumn("tile", TileWrapRasterRef($"value"))
        val mean = ds2.select(tileMean($"tile")).first()
        val doubleMean = ds2.select(tileMean(localAdd($"tile", $"tile"))).first()
        assert(2 * mean ===  doubleMean +- 0.0001)
      }
    }

    it("should handle multiple columns of bands with correctness") {
      import spark.implicits._

      val df = Seq((remoteCOGSingleband1.toASCIIString, remoteCOGSingleband2.toASCIIString))
        .toDF("col1", "col2")
        .select(ExpandNativeTiling(URIToRasterRef($"col1"), URIToRasterRef($"col2")).as(Seq("t1", "t2")))
        .cache()

      assert(df.select(GetExtent($"t1") === GetExtent($"t2")).as[Boolean].distinct().collect() === Array(true))

      assert(df.select(GetExtent($"t1")).distinct().count() === df.count())
    }

    it("should allow lazy application of a layer space") {
      import spark.implicits._
      new Fixture {
        val targetCRS = LatLng
        val targetExtent = fullRaster.extent.reproject(fullRaster.crs, targetCRS)
        val targetCellType = ByteConstantNoDataCellType
        val targetLayout = LayoutDefinition(targetExtent, TileLayout(10, 10, 100, 100))
        val space = LayerSpace(targetCRS, targetCellType, targetLayout)
        val ds = Seq((subRaster, subRaster)).toDF("src1", "src2")
        val projected = ds.asRF(space)
        val tile = projected.select($"src1".as[Tile]).first()
        assert(tile.isInstanceOf[ProjectedRasterTile])
        assert(tile.asInstanceOf[ProjectedRasterTile].sourceKind === SourceKind.Reference)
      }
    }
    it("should allow lazy projection into layer space") {
      import spark.implicits._
      new Fixture {
        val targetCRS = LatLng
        val targetExtent = subRaster.extent.reproject(subRaster.crs, targetCRS)
        val targetCellType = UByteConstantNoDataCellType
        val targetLayout = LayoutDefinition(targetExtent, TileLayout(4, 4, 10, 10))
        val space = LayerSpace(targetCRS, targetCellType, targetLayout)
        val ds = Seq(subRaster).toDF("src")
        val projected = ds.asRF(space)
        val tile = projected.select($"src".as[Tile]).first()
        assert(tile.isInstanceOf[ProjectedRasterTile])
        assert(tile.asInstanceOf[ProjectedRasterTile].sourceKind === SourceKind.Reference)
        //println(tile.statistics.map(CellStatistics.apply).map(_.asciiStats))
      }
    }
    it("should serialize") {
      new Fixture {
        import java.io._

        val buf = new java.io.ByteArrayOutputStream()
        val out = new ObjectOutputStream(buf)
        out.writeObject(subRaster)
        out.close()
        val data = buf.toByteArray
        val in = new ObjectInputStream(new ByteArrayInputStream(data))
        val recovered = in.readObject()
        assert(subRaster === recovered)
      }
    }
  }
}

object RasterRefSpec {
  case class ReadMonitor() extends ReadCallback with LazyLogging {
    var reads: Int = 0
    var total: Long = 0
    override def readRange(source: RasterSource, start: Long, length: Int): Unit = {
      logger.trace(s"Reading $length at $start from $source")
      // Ignore header reads
      if(start > 0) reads += 1
      total += length
    }

    override def toString: String = s"$productPrefix(reads=$reads, total=$total)"
  }
}
