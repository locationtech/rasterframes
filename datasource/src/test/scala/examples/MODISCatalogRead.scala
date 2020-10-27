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

import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.locationtech.rasterframes.util
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.datasource.raster._
import org.locationtech.rasterframes.util.DFWithPrettyPrint

object MODISCatalogRead extends App {
  implicit val spark = SparkSession.builder()
    .master("local[*]")
    .appName("MODIS")
    .config("spark.driver.extraJavaOptions", "-Drasterframes.prefer-gdal=true")
    .withKryoSerialization
    .getOrCreate()
  import spark.implicits._

  val cat_filename = "2018-07-04_scenes.txt"
  spark.sparkContext.addFile(s"https://modis-pds.s3.amazonaws.com/MCD43A4.006/$cat_filename")

  val modis_catalog = spark.read
    .format("csv")
    .option("header", "true")
    .load(SparkFiles.get(cat_filename))
    .withColumn("base_url", concat(regexp_replace($"download_url", "index.html$", ""), $"gid"))
    .drop("download_url")
    .withColumn("red", concat($"base_url", lit("_B01.TIF")))
    .withColumn("nir", concat($"base_url", lit("_B02.TIF")))

  util.time("count") {
    println("Available scenes: " + modis_catalog.count())
  }

  val equator = modis_catalog.where(col("gid").like("%v07%"))
  val rf = spark.read.raster.fromCatalog(equator, "red", "nir").load()
  val sample = rf
    .limit(10)
    .repartition(5)
    .select($"gid", rf_extent($"red"), rf_extent($"nir"), rf_tile($"red"), rf_tile($"nir"))
    .where(!rf_is_no_data_tile($"red"))
  util.time("show") {
    print(sample.toMarkdown())
  }
}
