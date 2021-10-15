/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Astraea, Inc.
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

package examples

import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import org.apache.spark.sql._
import org.locationtech.rasterframes._

/**
  * Boilerplate test run file
  *
  * @since 10/8/17
  */
object LocalArithmetic extends App {
  implicit val spark = SparkSession.builder()
    .master("local[*]")
    .appName(getClass.getName)
    .withKryoSerialization
    .getOrCreate()
    .withRasterFrames

    val filenamePattern = "L8-B%d-Elkton-VA.tiff"
    val bandNumbers = 1 to 4
    val bandColNames = bandNumbers.map(b => s"band_$b").toArray
    def readTiff(name: String): SinglebandGeoTiff = SinglebandGeoTiff(s"../samples/$name")

  val joinedRF = bandNumbers.
    map { b => (b, filenamePattern.format(b)) }.
    map { case (b, f) => (b, readTiff(f)) }.
    map { case (b, t) => t.projectedRaster.toLayer(s"band_$b") }.
    reduce(_ spatialJoin _)

  val addRF = joinedRF.withColumn("1+2", rf_local_add(joinedRF("band_1"), joinedRF("band_2"))).asLayer
  val divideRF = joinedRF.withColumn("1/2", rf_local_divide(joinedRF("band_1"), joinedRF("band_2"))).asLayer

  addRF.select("1+2").collect().apply(0) .getClass

  val raster = divideRF.select(rf_tile_sum(divideRF("1/2")),
    rf_tile_sum(joinedRF("band_1")), rf_tile_sum(joinedRF("band_2")))
  raster.show(1)
}