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

import geotrellis.raster.{Dimensions, RasterExtent}
import geotrellis.vector._
import org.apache.spark.sql.rf.RasterSourceUDT
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.util.GridHasGridBounds


class RasterSourceSpec extends TestEnvironment with TestData {
  def sub(e: Extent) = {
    val c = e.center
    val w = e.width
    val h = e.height
    Extent(c.x, c.y, c.x + w * 0.1, c.y + h * 0.1)
  }

  describe("General RasterSource") {
    it("should identify as UDT") {
      assert(new RasterSourceUDT() === new RasterSourceUDT())
    }
    val rs = RFRasterSource(getClass.getResource("/L8-B8-Robinson-IL.tiff").toURI)
    it("should compute nominal tile layout bounds") {
      val bounds = rs.layoutBounds(Dimensions(65, 60))
      val agg = bounds.reduce(_ combine _)
      agg should be (rs.gridBounds)
    }
    it("should compute nominal tile layout extents") {
      val extents = rs.layoutExtents(Dimensions(63, 63))
      val agg = extents.reduce(_ combine _)
      agg should be (rs.extent)
    }
    it("should reassemble correct grid from extents") {
      val dims = Dimensions(63, 63)
      val ext = rs.layoutExtents(dims).head
      val bounds = rs.layoutBounds(dims).head
      rs.rasterExtent.gridBoundsFor(ext) should be (bounds)
    }
    it("should compute layout extents from scene with fractional gsd") {

      val rs = RFRasterSource(remoteMODIS)

      val dims = rs.layoutExtents(NOMINAL_TILE_DIMS)
        .map(e => rs.rasterExtent.gridBoundsFor(e, false))
        .map(b => (b.width, b.height))
        .distinct
      forEvery(dims) { d =>
        d._1 should be <= NOMINAL_TILE_SIZE
        d._2 should be <= NOMINAL_TILE_SIZE
      }

      val re = RasterExtent(
        Extent(1.4455356755667E7, -3335851.5589995002, 1.55673072753335E7, -2223901.039333),
        2400, 2400
      )

      val divisions = re.gridBounds
        .split(256, 256)
        .map { gb => gb -> re.rasterExtentFor(gb) }
        .map { case (originalGB, t) => (originalGB, t.extent)  }
        .map { case (originalGB, e) => (originalGB, re.gridBoundsFor(e, clamp = false)) }
        .map { case (ogb, gb) => ((ogb.width, ogb.height), (gb.width, gb.height)) }
        .toSeq
        .distinct
        divisions.length should be(4)
    }
  }

  describe("HTTP RasterSource") {
    it("should support metadata querying over HTTP") {
      withClue("remoteCOGSingleband") {
        val src = RFRasterSource(remoteCOGSingleband1)
        assert(!src.extent.isEmpty)
      }
      withClue("remoteCOGMultiband") {
        val src = RFRasterSource(remoteCOGMultiband)
        assert(!src.extent.isEmpty)
      }
    }
    it("should read sub-tile") {
      withClue("remoteCOGSingleband") {
        val src = RFRasterSource(remoteCOGSingleband1)
        val raster = src.read(sub(src.extent))
        assert(raster.size > 0 && raster.size < src.size)
      }
      withClue("remoteCOGMultiband") {
        val src = RFRasterSource(remoteCOGMultiband)
        val raster = src.read(sub(src.extent))
        assert(raster.size > 0 && raster.size < src.size)
      }
    }
    it("should Java serialize") {
      import java.io._
      val src = RFRasterSource(remoteCOGSingleband1)
      val buf = new java.io.ByteArrayOutputStream()
      val out = new ObjectOutputStream(buf)
      out.writeObject(src)
      out.close()

      val data = buf.toByteArray
      val in = new ObjectInputStream(new ByteArrayInputStream(data))
      val recovered = in.readObject().asInstanceOf[RFRasterSource]
      assert(src.toString === recovered.toString)
    }
  }
  describe("File RasterSource") {
    it("should support metadata querying of file") {
      val localSrc = geotiffDir.resolve("LC08_B7_Memphis_COG.tiff").toUri
      val src = RFRasterSource(localSrc)
      assert(!src.extent.isEmpty)
    }
    it("should interpret no scheme as file://"){
      val localSrc = geotiffDir.resolve("LC08_B7_Memphis_COG.tiff").toString
      val schemelessUri = new URI(localSrc)
      schemelessUri.getScheme should be (null)
      val src = RFRasterSource(schemelessUri)
      assert(!src.extent.isEmpty)
    }
  }

  if(GDALRasterSource.hasGDAL) {
    describe("GDAL Rastersource") {
      val gdal = GDALRasterSource(cogPath)
      val jvm = JVMGeoTiffRasterSource(cogPath)
      it("should compute the same metadata as JVM RasterSource") {
        gdal.cellType should be(jvm.cellType)
      }
      it("should compute the same dimensions as JVM RasterSource") {
        val dims = Dimensions(128, 128)
        gdal.extent should be(jvm.extent)
        gdal.rasterExtent should be(jvm.rasterExtent)
        gdal.cellSize should be(jvm.cellSize)
        gdal.layoutBounds(dims) should contain allElementsOf jvm.layoutBounds(dims)
        gdal.layoutExtents(dims) should contain allElementsOf jvm.layoutExtents(dims)
      }


      it("should support vsi file paths") {
        val archivePath = geotiffDir.resolve("L8-archive.zip")
        val archiveURI = URI.create("gdal://vsizip/" + archivePath.toString + "/L8-RGB-VA.tiff")
        val gdal = GDALRasterSource(archiveURI)

        gdal.bandCount should be (3)
      }

      it("should support nested vsi file paths") {
        val path = URI.create("gdal://vsihdfs/hdfs://dp-01.tap-psnc.net:9000/user/dpuser/images/landsat/LC081900242018092001T1-SC20200409091832/LC08_L1TP_190024_20180920_20180928_01_T1_sr_band1.tif")
        assert(RFRasterSource(path).isInstanceOf[GDALRasterSource])
      }

      it("should interpret no scheme as file://") {
        val localSrc = geotiffDir.resolve("LC08_B7_Memphis_COG.tiff").toString
        val schemelessUri = new URI(localSrc)
        val gdal = GDALRasterSource(schemelessUri)
        val jvm = JVMGeoTiffRasterSource(schemelessUri)
        gdal.extent should be (jvm.extent)
        gdal.cellSize should be(jvm.cellSize)
      }
    }
  }

  describe("RasterSource tile construction") {
    it("should read all tiles") {
      val src = RFRasterSource(remoteMODIS)

      val subrasters = src.readAll()

      val collected = subrasters.map(_.extent).reduceLeft(_.combine(_))

      assert(src.extent.xmin === collected.xmin +- 0.01)
      assert(src.extent.ymin === collected.ymin +- 0.01)
      assert(src.extent.xmax === collected.xmax +- 0.01)
      assert(src.extent.ymax === collected.ymax +- 0.01)

      val totalCells = subrasters.map(_.size).sum

      assert(totalCells === src.size)
    }
  }
}
