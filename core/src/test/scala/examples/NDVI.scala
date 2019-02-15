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
import java.nio.file.{Files, Paths}

import astraea.spark.rasterframes._
import geotrellis.raster._
import geotrellis.raster.render._
import geotrellis.raster.io.geotiff.{GeoTiff, SinglebandGeoTiff}
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object NDVI extends App {

  def readTiff(name: String) =
    SinglebandGeoTiff(IOUtils.toByteArray(getClass.getResourceAsStream(s"/$name")))

  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName(getClass.getName)
    .withKryoSerialization
    .getOrCreate()
    .withRasterFrames

  println(spark.sparkContext.hadoopConfiguration)
  println(spark.sqlContext.sparkSession.conf.getAll )

  import spark.implicits._

  def redBand = readTiff("L8-B4-Elkton-VA.tiff").projectedRaster.toRF("red_band")
  def nirBand = readTiff("L8-B5-Elkton-VA.tiff").projectedRaster.toRF("nir_band")

  val ndvi = udf((red: Tile, nir: Tile) => {
    val redd = red.convert(DoubleConstantNoDataCellType)
    val nird = nir.convert(DoubleConstantNoDataCellType)
    (nird - redd) / (nird + redd)
  })

  val rf = redBand.spatialJoin(nirBand).withColumn("ndvi", ndvi($"red_band", $"nir_band")).asRF

  rf.printSchema()

  val pr = rf.toRaster($"ndvi", 233, 214)
  GeoTiff(pr).write("ndvi.tiff")

  val brownToGreen = ColorRamp(RGBA(166, 97, 26, 255), RGBA(223, 194, 125, 255),
    RGBA(245, 245, 245, 255), RGBA(128, 205, 193, 255), RGBA(1, 133, 113, 255))
    .stops(128)

  val colors = ColorMap.fromQuantileBreaks(pr.tile.histogramDouble(), brownToGreen)

  Files.createDirectories(Paths.get("target/scala-2.11/tut"))
  pr.tile.color(colors).renderPng().write("target/scala-2.11/tut/rf-ndvi.png")

  spark.stop()
}
