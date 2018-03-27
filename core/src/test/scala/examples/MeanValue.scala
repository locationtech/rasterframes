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

import astraea.spark.rasterframes._
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Compute the cell mean value of an image.
 *
 * @since 10/23/17
 */
object MeanValue extends App {

  implicit val spark = SparkSession.builder()
    .master("local[*]")
    .appName(getClass.getName)
    .getOrCreate()
    .withRasterFrames


  val scene = SinglebandGeoTiff("src/test/resources/L8-B8-Robinson-IL.tiff")

  val rf = scene.projectedRaster.toRF(128, 128) // <-- tile size

  rf.printSchema

  val tileCol = rf("tile")
  rf.agg(aggNoDataCells(tileCol), aggDataCells(tileCol), aggMean(tileCol)).show(false)

  spark.stop()
}
