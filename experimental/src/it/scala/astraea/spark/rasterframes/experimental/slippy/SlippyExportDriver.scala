/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea. Inc.
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
 *
 */

package astraea.spark.rasterframes.experimental.slippy

import java.io.File

import astraea.spark.rasterframes._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import org.apache.spark.sql.SparkSession
import SlippyExport._
import astraea.spark.rasterframes.util.MultibandRender

object SlippyExportDriver {
  def main(args: Array[String]): Unit = {

    implicit val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("RasterFrames")
      .getOrCreate()
      .withRasterFrames

    def mergeBands = {
      val bands: Seq[SinglebandGeoTiff] = for (i ← 1 to 3) yield {
        TestData.l8Sample(i)
      }

      val mtile = MultibandTile(bands.map(_.tile))

      val pr = ProjectedRaster(mtile, bands.head.extent, bands.head.crs)

      implicit val bandCount = PairRDDConverter.forSpatialMultiband(bands.length)

      val rf = pr.toRF(64, 64)

      //rf.exportGeoTiffTiles(new File("target/slippy-tiff").toURI)

      rf.exportSlippyMap(new File("target/slippy-1/").toURI)
    }

   def multiband = {
     implicit val bandCount = PairRDDConverter.forSpatialMultiband(3)
     val rf = TestData.rgbCogSample.projectedRaster.toRF(256, 256)
     import astraea.spark.rasterframes.datasource.geotiff._
     //val rf = spark.read.geotiff.loadRF(getClass.getResource("/LC08_RGB_Norfolk_COG.tiff").toURI)
     println(rf.tileLayerMetadata.merge.toString)
     rf.exportSlippyMap(new File("target/slippy-2/").toURI, MultibandRender.Landsat8NaturalColor)
    }

    multiband
    //mergeBands
  }
}
