/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2020 Astraea, Inc.
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

package examples

import geotrellis.proj4.LatLng
import org.apache.spark.sql.SparkSession
import org.locationtech.rasterframes.datasource.raster._
import org.locationtech.rasterframes.{util, _}
import org.locationtech.rasterframes.util.DFWithPrettyPrint

object L8SceneRead extends App {
  implicit val spark = SparkSession.builder()
    .master("local[*]")
    .appName("MODIS")
    .config("spark.driver.extraJavaOptions", "-Drasterframes.prefer-gdal=true")
    .withKryoSerialization
    .getOrCreate()
    .withRasterFrames

  import spark.implicits._
  val bands = Seq(4, 5)
  val uris = for {
    b <- bands
  } yield f"https://landsat-pds.s3.us-west-2.amazonaws.com/c1/L8/014/032/LC08_L1TP_014032_20190720_20190731_01_T1/LC08_L1TP_014032_20190720_20190731_01_T1_B${b}.TIF"

  val catalog = Seq(
    bands.map("B" + _).mkString(","),
    uris.mkString(",")
  ).mkString("\n")

  val rf = spark.read.raster.fromCSV(catalog).load().cache()
    .limit(20)
    .repartition(20)
    .select($"B4" as "red", $"B5" as "NIR")
    .withColumn("NDVI", rf_normalized_difference($"NIR", $"red"))
    .where(rf_tile_sum($"NDVI") > 10000)
    .withColumn("longitude_latitude",
      st_reproject(st_centroid(rf_geometry($"red")), rf_crs($"red"), LatLng))
    .select($"longitude_latitude", $"red", $"NIR", $"NDVI")

  util.time("markdown") {
    print(rf.toMarkdown())
  }
}
