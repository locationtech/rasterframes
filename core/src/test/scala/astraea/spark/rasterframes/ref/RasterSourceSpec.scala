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

import java.net.URI

import astraea.spark.rasterframes.TestEnvironment
import astraea.spark.rasterframes.ref.RasterSource.HttpGeoTiffRasterSource

/**
 *
 *
 * @since 8/22/18
 */
class RasterSourceSpec extends TestEnvironment {
  val example1 = URI.create("https://s3-us-west-2.amazonaws.com/landsat-pds/c1/L8/149/039/LC08_L1TP_149039_20170411_20170415_01_T1/LC08_L1TP_149039_20170411_20170415_01_T1_B4.TIF")

  val example2 =  URI.create("https://s3-us-west-2.amazonaws.com/radiant-nasa-iserv/2014/02/14/IP0201402141023382027S03100E/IP0201402141023382027S03100E-COG.tif")

  describe("RasterSource") {
     it("Should support metadata querying over HTTP") {
       withClue("example1") {
         val src = HttpGeoTiffRasterSource(example1)
         assert(!src.extent.isEmpty)
       }
       withClue("example2") {
         val src = HttpGeoTiffRasterSource(example2)
         assert(!src.extent.isEmpty)
       }
     }
  }
}
