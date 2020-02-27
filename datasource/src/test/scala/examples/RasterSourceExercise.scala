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

import java.net.URI

import geotrellis.raster._
import org.apache.spark.sql.SparkSession
import org.locationtech.rasterframes.ref.RFRasterSource

object RasterSourceExercise extends App {
  val path = "s3://sentinel-s2-l2a/tiles/22/L/EP/2019/5/31/0/R60m/B08.jp2"


  implicit val spark = SparkSession.builder().
    master("local[*]").appName("Hit me").getOrCreate()

  spark.range(1000).rdd
    .map(_ => path)
    .flatMap(uri => {
      val rs = RFRasterSource(URI.create(uri))
      val grid = GridBounds(0, 0, rs.cols - 1, rs.rows - 1)
      val tileBounds = grid.split(256, 256).toSeq
      rs.readBounds(tileBounds, Seq(0))
    })
    .foreach(_ => ())

}
