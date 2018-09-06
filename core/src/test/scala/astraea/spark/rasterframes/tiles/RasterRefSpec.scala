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

package astraea.spark.rasterframes.tiles

import astraea.spark.rasterframes._
import astraea.spark.rasterframes.ref.RasterSource.ReadCallback
import astraea.spark.rasterframes.ref.{LayerSpace, RasterRef, RasterSource}
import astraea.spark.rasterframes.tiles.RasterRefSpec.ReadMonitor
import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.LatLng
import geotrellis.raster.{ByteConstantNoDataCellType, ShortConstantNoDataCellType, TileLayout}
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
    val src = RasterSource(remoteCOGSingleband, Some(counter))
    val fullRaster = RasterRef(src)
    val subExtent = sub(src.extent)
    val subRaster = RasterRef(src, subExtent)
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
        subRaster.tile.rescale(0, 255).renderPng().write("target/foo1.png")
      }
    }
    it("should be realizable") {
      new Fixture {
        assert(counter.reads === 0)
        assert(subRaster.tile.statistics.map(_.dataCells) === Some(subRaster.cols * subRaster.rows))
        assert(counter.reads > 0)
      }
    }
    it("should be reprojectable") {
      new Fixture {
        val reprojected = subRaster.tile.reproject(LatLng)
        assert(counter.reads === 0)
        reprojected.rescale(0, 255).renderPng().write("target/foo2.png")
        assert(counter.reads > 0)
      }
    }

    it("should be Dataset compatible") {
      import spark.implicits._
      new Fixture {
        val ds = Seq(subRaster).toDS()
        assert(ds.first().isInstanceOf[RasterRef])
        val ds2 = ds.withColumn("tile", resolveRasterRef($"value"))
        val mean = ds2.select(tileMean($"tile")).first()
        val doubleMean = ds2.select(tileMean(localAdd($"tile", $"tile"))).first()
        assert(2 * mean ===  doubleMean +- 0.0001)
      }
    }
    it("should allow application of a layer space") {
      new Fixture {
        val targetCRS = LatLng
        val targetExtent = fullRaster.extent.reproject(fullRaster.crs, targetCRS)
        val targetCellType = ByteConstantNoDataCellType
        val targetLayout = LayoutDefinition(targetExtent, TileLayout(10, 10, 100, 100))

        val space = LayerSpace(targetCRS, targetCellType, targetLayout)
        val subs = space.project(fullRaster)
        assert(subs.size === 100)
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
    override def readRange(source: RasterSource, start: Long, length: Int): Unit = {
      logger.trace(s"Reading $length at $start from $source")
      // Ignore header reads
      if(start > 0) reads += 1
    }

    override def toString: String = s"$productPrefix(reads=$reads)"
  }
}
