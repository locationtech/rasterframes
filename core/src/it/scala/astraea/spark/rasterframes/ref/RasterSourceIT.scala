/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2019 Astraea, Inc.
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

import java.net.URI

import astraea.spark.rasterframes
import astraea.spark.rasterframes.util.time
import astraea.spark.rasterframes.{TestData, TestEnvironment}
import geotrellis.vector.Extent

/**
 *
 *
 * @since 8/22/18
 */
class RasterSourceIT extends TestEnvironment with TestData {
  def sub(e: Extent) = {
    val c = e.center
    val w = e.width
    val h = e.height
    Extent(c.x, c.y, c.x + w * 0.1, c.y + h * 0.1)
  }

  describe("RasterSource.readAll") {
    it("should return consistently ordered tiles across bands for a given scene") {
      time(s"two band comparison prefer-gdal=${ rasterframes.rfConfig.getBoolean("prefer-gdal")}") {
        // These specific scenes exhibit the problem where we see different subtile segment ordering across the bands of a given scene.
        val rURI = new URI(
          "https://s3-us-west-2.amazonaws.com/landsat-pds/c1/L8/016/034/LC08_L1TP_016034_20181003_20181003_01_RT/LC08_L1TP_016034_20181003_20181003_01_RT_B4.TIF")
        val bURI = new URI(
          "https://s3-us-west-2.amazonaws.com/landsat-pds/c1/L8/016/034/LC08_L1TP_016034_20181003_20181003_01_RT/LC08_L1TP_016034_20181003_20181003_01_RT_B2.TIF")
        val red = time("read B4") {
          RasterSource(rURI).readAll()
        }
        val blue = time("read B2") {
          RasterSource(bURI).readAll()
        }
        time("test empty") {
          red should not be empty
        }
        time("compare sizes") {
          red.size should equal(blue.size)
        }
        time("compare dimensions") {
          red.map(_.dimensions) should contain theSameElementsAs blue.map(_.dimensions)
        }
      }
    }
  }
}
