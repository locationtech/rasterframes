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
 */
package astraea.spark.rasterframes.datasource.geotiff

import astraea.spark.rasterframes._

/**
 * @since 1/14/18
 */
class GeoTiffDataSourceSpec
    extends TestEnvironment with TestData
    with IntelliJPresentationCompilerHack {

  val cogPath = getClass.getResource("/LC08_RGB_Norfolk_COG.tiff").toURI

  describe("GeoTiff reading") {

    it("should read sample GeoTiff") {
      val rf = spark.read
        .geotiff
        .loadRF(cogPath)

      assert(rf.count() > 10)
    }

    it("should write RF to parquet") {
      val rf = spark.read
        .geotiff
        .loadRF(cogPath)
      assert(write(rf))
    }
  }
}
