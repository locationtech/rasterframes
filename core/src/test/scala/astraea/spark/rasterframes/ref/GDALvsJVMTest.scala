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
import astraea.spark.rasterframes.{TestData, TestEnvironment}
import geotrellis.contrib.vlm.gdal.GDALRasterSource
import geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource

class GDALvsJVMTest extends TestEnvironment with TestData {
  describe("GDAL RasterSource vs JVM RasterSource") {
    it("should compute the same extent") {
      val imgPath = getClass.getResource("/L8-B1-Elkton-VA.tiff").toURI.getPath
      val jvm = GeoTiffRasterSource(imgPath)
      val gdal = GDALRasterSource(imgPath)
      gdal.extent should be (jvm.extent)
    }
  }
}
