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

package org.locationtech.rasterframes.ref

import java.net.URI

import geotrellis.raster.{ByteConstantNoDataCellType, Tile}
import geotrellis.vector._
import org.apache.spark.SparkException
import org.apache.spark.sql.Encoders
import org.locationtech.rasterframes.{TestEnvironment, _}
import org.locationtech.rasterframes.expressions.accessors._
import org.locationtech.rasterframes.expressions.generators._
import org.locationtech.rasterframes.ref.RasterRef.RasterRefTile
import org.locationtech.rasterframes.tiles.ProjectedRasterTile

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
    val src = RasterSource(remoteCOGSingleband1)
    val fullRaster = RasterRef(src, 0, None, None)
    val subExtent = sub(src.extent)
    val subRaster = RasterRef(src, 0, Some(subExtent), Some(src.rasterExtent.gridBoundsFor(subExtent)))
  }

  import spark.implicits._

  implicit val enc = Encoders.tuple(Encoders.scalaInt, RasterRef.rrEncoder)
  describe("GetCRS Expression") {
    it("should read from RasterRef") {
      new Fixture {
        val ds = Seq((1, fullRaster)).toDF("index", "ref")
        val crs = ds.select(GetCRS($"ref"))
        assert(crs.count() === 1)
        assert(crs.first() !== null)
      }
    }
    it("should read from sub-RasterRef") {
      new Fixture {
        val ds = Seq((1, subRaster)).toDF("index", "ref")
        val crs = ds.select(GetCRS($"ref"))
        assert(crs.count() === 1)
        assert(crs.first() !== null)
      }
    }
  }

  describe("GetDimensions Expression") {
    it("should read from RasterRef") {
      new Fixture {
        val ds = Seq((1, fullRaster)).toDF("index", "ref")
        val dims = ds.select(GetDimensions($"ref"))
        assert(dims.count() === 1)
        assert(dims.first() !== null)
      }
    }
    it("should read from sub-RasterRef") {
      new Fixture {
        val ds = Seq((1, subRaster)).toDF("index", "ref")
        val dims = ds.select(GetDimensions($"ref"))
        assert(dims.count() === 1)
        assert(dims.first() !== null)
      }
    }

    it("should read from RasterRefTile") {
      new Fixture {
        val ds = Seq((1, RasterRefTile(fullRaster): Tile)).toDF("index", "ref")
        val dims = ds.select(GetDimensions($"ref"))
        assert(dims.count() === 1)
        assert(dims.first() !== null)
      }
    }
    it("should read from sub-RasterRefTiles") {
      new Fixture {
        val ds = Seq((1, RasterRefTile(subRaster): Tile)).toDF("index", "ref")
        val dims = ds.select(GetDimensions($"ref"))
        assert(dims.count() === 1)
        assert(dims.first() !== null)
      }
    }
  }

  describe("GetExtent") {
    it("should read from RasterRef") {
      import spark.implicits._
      new Fixture {
        val ds = Seq((1, fullRaster)).toDF("index", "ref")
        val extent = ds.select(rf_extent($"ref"))
        assert(extent.count() === 1)
        assert(extent.first() !== null)
      }
    }
    it("should read from sub-RasterRef") {
      import spark.implicits._
      new Fixture {
        val ds = Seq((1, subRaster)).toDF("index", "ref")
        val extent = ds.select(rf_extent($"ref"))
        assert(extent.count() === 1)
        assert(extent.first() !== null)
      }
    }
  }

  describe("RasterRef") {
    it("should delay reading") {
      new Fixture {
        assert(subRaster.cellType === src.cellType)
      }
    }
    it("should support subextents") {
      new Fixture {
        assert(subRaster.cols.toDouble === src.cols * 0.01 +- 2.0)
        assert(subRaster.rows.toDouble === src.rows * 0.01 +- 2.0)
        //subRaster.tile.rescale(0, 255).renderPng().write("target/foo1.png")
      }
    }
    it("should be realizable") {
      new Fixture {
        assert(subRaster.tile.statistics.map(_.dataCells) === Some(subRaster.cols * subRaster.rows))
      }
    }

    it("should Java serialize") {
      new Fixture {
        import java.io._

        val buf = new java.io.ByteArrayOutputStream()
        val out = new ObjectOutputStream(buf)
        out.writeObject(subRaster)
        out.close()
        val data = buf.toByteArray
        val in = new ObjectInputStream(new ByteArrayInputStream(data))
        val recovered = in.readObject()
        subRaster should be (recovered)
      }
    }
  }

  describe("RasterRef creation") {
    it("should realize subiles of proper size") {
      val src = RasterSource(remoteMODIS)
      val dims = src
        .layoutExtents(NOMINAL_TILE_DIMS)
        .map(e => RasterRef(src, 0, Some(e), None))
        .map(_.dimensions)
        .distinct

      forEvery(dims) { d =>
        d._1 should be <= NOMINAL_TILE_SIZE
        d._2 should be <= NOMINAL_TILE_SIZE
      }
    }
  }

  describe("RasterSourceToRasterRefs") {
    it("should convert and expand RasterSource") {
      val src = RasterSource(remoteMODIS)
      import spark.implicits._
      val df = Seq(src).toDF("src")
      val refs = df.select(RasterSourceToRasterRefs(None, Seq(0), $"src"))
      refs.count() should be (1)
    }

    it("should properly realize subtiles") {
      val src = RasterSource(remoteMODIS)
      import spark.implicits._
      val df = Seq(src).toDF("src")
      val refs = df.select(RasterSourceToRasterRefs(Some(NOMINAL_TILE_DIMS), Seq(0), $"src") as "proj_raster")

      refs.count() shouldBe > (1L)

      val dims = refs.select(rf_dimensions($"proj_raster")).distinct().collect()
      forEvery(dims) { r =>
        r.cols should be <= NOMINAL_TILE_SIZE
        r.rows should be <= NOMINAL_TILE_SIZE
      }
    }
    it("should throw exception on invalid URI") {
      val src = RasterSource(URI.create("http://foo/bar"))
      import spark.implicits._
      val df = Seq(src).toDF("src")
      val refs = df.select(RasterSourceToRasterRefs($"src") as "proj_raster")
      logger.warn(Console.REVERSED + "Upcoming 'java.lang.IllegalArgumentException' expected in logs." + Console.RESET)
      assertThrows[SparkException] {
        refs.first()
      }
    }
  }

  describe("RealizeTile") {
    it("should pass through basic Tile") {
      val t = TestData.randomTile(5, 5, ByteConstantNoDataCellType)
      val result = Seq(t).toDF("tile").select(rf_tile($"tile")).first()
      assertEqual(result, t)
    }

    it("should simplify ProjectedRasterTile") {
      val t = TestData.randNDPRT
      val result = Seq(t).toDF("tile").select(rf_tile($"tile")).first()
      result.isInstanceOf[ProjectedRasterLike] should be (false)
      assertEqual(result, t.toArrayTile())
    }

    it("should resolve a RasterRef") {
      new Fixture {
        import RasterRef.rrEncoder // This shouldn't be required, but product encoder gets choosen.
        val r: RasterRef = subRaster
        val result = Seq(r).toDF("ref").select(rf_tile($"ref")).first()
        result.isInstanceOf[RasterRefTile] should be(false)
        assertEqual(r.tile.toArrayTile(), result)
      }
    }

    it("should resolve a RasterRefTile") {
      new Fixture {
        val t: ProjectedRasterTile = RasterRefTile(subRaster)
        val result = Seq(t).toDF("tile").select(rf_tile($"tile")).first()
        result.isInstanceOf[RasterRefTile] should be(false)
        assertEqual(t.toArrayTile(), result)
      }
    }

    it("should construct a RasterRefTile without I/O") {
      new Fixture {
        // SimpleRasterInfo is a proxy for header data requests.
        val start = SimpleRasterInfo.cacheStats.hitCount()
        val t: ProjectedRasterTile = RasterRefTile(subRaster)
        val result = Seq(t, subRaster.tile).toDF("tile").first()
        val end = SimpleRasterInfo.cacheStats.hitCount()
        end should be(start)
      }
    }
  }
}
