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

import java.net.URI

import geotrellis.proj4.LatLng
import geotrellis.vector.Extent
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.expressions.aggregates.TileRasterizerAggregate

class RasterRefIT extends TestEnvironment {
  describe("practical subregion reads") {
    ignore("should construct a natural color composite") {
      import spark.implicits._
      def scene(idx: Int) = URI.create(s"https://landsat-pds.s3.us-west-2.amazonaws.com" +
        s"/c1/L8/176/039/LC08_L1TP_176039_20190703_20190718_01_T1/LC08_L1TP_176039_20190703_20190718_01_T1_B$idx.TIF")

      val redScene = RasterSource(scene(4))
      // [west, south, east, north]
      val area = Extent(31.115, 29.963, 31.148, 29.99).reproject(LatLng, redScene.crs)

      val red = RasterRef(redScene, 0, Some(area), None)
      val green = RasterRef(RasterSource(scene(3)), 0, Some(area), None)
      val blue = RasterRef(RasterSource(scene(2)), 0, Some(area), None)

      val rf = Seq((red, green, blue)).toDF("red", "green", "blue")
      val df = rf.select(
        rf_crs($"red"), rf_extent($"red"), rf_tile($"red"), rf_tile($"green"), rf_tile($"blue"))
        .toDF

      val raster = TileRasterizerAggregate(df, redScene.crs, None, None)

      forEvery(raster.tile.statisticsDouble) { stats =>
        stats should be ('defined)
        stats.get.dataCells shouldBe > (1000L)
      }
      
      //import geotrellis.raster.io.geotiff.{GeoTiffOptions, MultibandGeoTiff, Tiled}
      //import geotrellis.raster.io.geotiff.compression.{DeflateCompression, NoCompression}
      //import geotrellis.raster.io.geotiff.tags.codes.ColorSpace
      //val tiffOptions = GeoTiffOptions(Tiled,  DeflateCompression, ColorSpace.RGB)
      //MultibandGeoTiff(raster, raster.crs, tiffOptions).write("target/composite.tif")
    }
  }
}