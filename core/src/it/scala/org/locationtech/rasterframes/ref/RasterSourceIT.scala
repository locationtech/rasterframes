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

package org.locationtech.rasterframes.ref

import java.lang.Math.ceil
import java.net.URI

import com.azavea.gdal.GDALWarp
import org.locationtech.rasterframes
import org.locationtech.rasterframes.util.time
import org.locationtech.rasterframes.{NOMINAL_TILE_SIZE, TestData, TestEnvironment}

/**
 *
 *
 * @since 8/22/18
 */
class RasterSourceIT extends TestEnvironment with TestData {

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

  if (RasterSource.IsGDAL.hasGDAL) {
    println("GDAL version: " + GDALWarp.get_version_info("--version").trim())

    describe("GDAL support") {


      it("should read JPEG2000 scene") {
        RasterSource(localSentinel).readAll().flatMap(_.tile.statisticsDouble).size should be(64)
      }

      it("should read small MRF scene with one band converted from MODIS HDF") {
        val (expectedTileCount, _) = expectedTileCountAndBands(2400, 2400)
        RasterSource(modisConvertedMrfPath).readAll().flatMap(_.tile.statisticsDouble).size should be (expectedTileCount)
      }

      it("should read remote HTTP MRF scene") {
        val (expectedTileCount, bands) = expectedTileCountAndBands(6257, 7584, 4)
        RasterSource(remoteHttpMrfPath).readAll(bands = bands).flatMap(_.tile.statisticsDouble).size should be (expectedTileCount)
      }

      it("should read remote S3 MRF scene") {
        val (expectedTileCount, bands) = expectedTileCountAndBands(6257, 7584, 4)
        RasterSource(remoteS3MrfPath).readAll(bands = bands).flatMap(_.tile.statisticsDouble).size should be (expectedTileCount)
      }
    }
  } else {
    describe("GDAL missing error support") {
      it("should throw exception reading JPEG2000 scene") {
        intercept[IllegalArgumentException] {
          RasterSource(localSentinel)
        }
      }

      it("should throw exception reading MRF scene with one band converted from MODIS HDF") {
        intercept[IllegalArgumentException] {
          RasterSource(modisConvertedMrfPath)
        }
      }

      it("should throw exception reading remote HTTP MRF scene") {
        intercept[IllegalArgumentException] {
          RasterSource(remoteHttpMrfPath)
        }
      }

      it("should throw exception reading remote S3 MRF scene") {
        intercept[IllegalArgumentException] {
          RasterSource(remoteS3MrfPath)
        }
      }
    }
  }

  private def expectedTileCountAndBands(x:Int, y:Int, bandCount:Int = 1) = {
    val imageDimensions = Seq(x.toDouble, y.toDouble)
    val tilesPerBand = imageDimensions.map(x ⇒ ceil(x / NOMINAL_TILE_SIZE)).product
    val bands = Range(0, bandCount)
    val expectedTileCount = tilesPerBand * bands.length
    (expectedTileCount, bands)
  }

}
