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
import org.locationtech.rasterframes.datasource.raster._
import org.locationtech.rasterframes.util.DFWithPrettyPrint
import org.locationtech.rasterframes.{util, _}

object NAIPMultibandRead extends App {
  implicit val spark = SparkSession.builder()
    .master("local[*]")
    .appName("MODIS")
    .config("spark.driver.extraJavaOptions", "-Drasterframes.prefer-gdal=true")
    .withKryoSerialization
    .getOrCreate()
    .withRasterFrames

  val mb = spark.read.raster.from("https://rasterframes.s3.amazonaws.com/samples/naip/m_3807863_nw_17_1_20160620.tif").withBandIndexes(0, 1, 2, 3).load()
  util.time("markdown") {
    print(mb.toMarkdown())
  }
}
