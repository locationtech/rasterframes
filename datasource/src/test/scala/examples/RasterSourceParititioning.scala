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

import org.apache.spark.sql.SparkSession
import org.locationtech.rasterframes
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.util.debug._
import org.locationtech.rasterframes.datasource.raster._
import org.locationtech.rasterframes.util.time

object RasterSourceParititioning extends App {
  implicit val spark = SparkSession.builder().
    master("local[*]").appName("RasterSource").getOrCreate()
  spark.sparkContext.setLogLevel("INFO")
  import spark.implicits._
  val catFile = getClass.getResource("/test-catalog.csv")

  val catalog = spark.read.option("header", true).csv(catFile.getPath)
  val rf = spark.read.raster.fromCatalog(catalog, "B1").load()
  rf.rdd.describePartitions

  time("count") {
    println(rf.count())
  }

  time("dims") {
    println(rf.select(rf_dimensions($"B1")).distinct().count())
  }

  System.in.read()
}
