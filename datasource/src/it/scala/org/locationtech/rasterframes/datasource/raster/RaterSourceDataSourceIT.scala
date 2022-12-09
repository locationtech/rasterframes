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

package org.locationtech.rasterframes.datasource.raster

import org.locationtech.rasterframes._

class RaterSourceDataSourceIT extends TestEnvironment with TestData {

  describe("RasterJoin Performance") {
    import spark.implicits._
    ignore("joining classification raster against L8 should run in a reasonable amount of time") {
      // A regression test.
      val rf = spark.read.raster
        .withSpatialIndex()
        .load("https://rasterframes.s3.amazonaws.com/samples/water_class/seasonality_90W_50N.tif")

      val target_rf =
        rf.select(rf_extent($"proj_raster").alias("extent"), rf_crs($"proj_raster").alias("crs"), rf_tile($"proj_raster").alias("target"))

      val cat =
      s"""
      B3,B5
      ${remoteCOGSingleband1},${remoteCOGSingleband2}
      """

      val features_rf = spark.read.raster
        .fromCSV(cat, "B3", "B5")
        .withSpatialIndex()
        .load()
        .withColumn("extent", rf_extent($"B3"))
        .withColumn("crs", rf_crs($"B3"))
        .withColumn("B3", rf_tile($"B3"))
        .withColumn("B5", rf_tile($"B5"))
        .withColumn("ndwi", rf_normalized_difference($"B3", $"B5"))
        .where(!rf_is_no_data_tile($"B3"))
        .select("extent", "crs", "ndwi")

      features_rf.explain(true)

      val joined = target_rf.rasterJoin(features_rf).cache()
      joined.show(false)
      //println(joined.select("ndwi").toMarkdown())
    }
  }
}
