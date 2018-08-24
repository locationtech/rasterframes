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

import astraea.spark.rasterframes.{TestData, TestEnvironment}
import geotrellis.vector.Extent

/**
 *
 *
 * @since 8/22/18
 */
class RasterSourceSpec extends TestEnvironment with TestData {
  def sub(e: Extent) = {
    val c = e.center
    val w = e.width
    val h = e.height
    Extent(c.x, c.y, c.x + w * 0.1, c.y + h * 0.1)
  }

  describe("HTTP RasterSource") {
    it("should support metadata querying over HTTP") {
      withClue("remoteCOGSingleband") {
        val src = RasterSource(remoteCOGSingleband)
        assert(!src.extent.isEmpty)
      }
      withClue("remoteCOGMultiband") {
        val src = RasterSource(remoteCOGMultiband)
        assert(!src.extent.isEmpty)
      }
    }
    it("should read sub-tile") {
      withClue("remoteCOGSingleband") {
        val src = RasterSource(remoteCOGSingleband)
        val Left(raster) = src.read(sub(src.extent))
        assert(raster.size > 0 && raster.size < src.size)
      }
      withClue("remoteCOGMultiband") {
        val src = RasterSource(remoteCOGMultiband)
        //println("CoG size", src.size, src.dimensions)
        val Right(raster) = src.read(sub(src.extent))
        //println("Subtile size", raster.size, raster.dimensions)
        assert(raster.size > 0 && raster.size < src.size)
      }
    }
    it("should serialize") {
      import java.io._
      val src = RasterSource(remoteCOGSingleband)
      val buf = new java.io.ByteArrayOutputStream()
      val out = new ObjectOutputStream(buf)
      out.writeObject(src)
      out.close()

      val data = buf.toByteArray
      val in = new ObjectInputStream(new ByteArrayInputStream(data))
      val recovered = in.readObject().asInstanceOf[RasterSource]
      assert(src.toString === recovered.toString)
    }
  }
  describe("File RasterSource") {
    it("should support metadata querying of file") {
      val localSrc = geotiffDir.resolve("LC08_B7_Memphis_COG.tiff").toUri
      val src = RasterSource(localSrc)
      println(src)
      assert(!src.extent.isEmpty)
    }
  }
}
