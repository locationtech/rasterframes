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

import geotrellis.raster._
import geotrellis.vector.Extent
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.datasource.raster._
import org.locationtech.rasterframes.encoders.CatalystSerializer._

object ExplodeWithLocation extends App {

  implicit val spark = SparkSession.builder()
    .master("local[*]").appName("RasterFrames")
    .withKryoSerialization.getOrCreate().withRasterFrames
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val example = "https://raw.githubusercontent.com/locationtech/rasterframes/develop/core/src/test/resources/LC08_B7_Memphis_COG.tiff"
  val rf = spark.read.raster.from(example).withTileDimensions(16, 16).load()

  val grid2map = udf((encExtent: Row, encDims: Row, colIdx: Int, rowIdx: Int) => {
    val extent = encExtent.to[Extent]
    val dims = encDims.to[Dimensions[Int]]
    GridExtent(extent, dims.cols, dims.rows).gridToMap(colIdx, rowIdx)
  })

  val exploded = rf
    .withColumn("dims", rf_dimensions($"proj_raster"))
    .withColumn("extent", rf_extent($"proj_raster"))
    .select(rf_explode_tiles($"proj_raster"), $"dims", $"extent")
    .select(grid2map($"extent", $"dims", $"column_index", $"row_index") as "location", $"proj_raster" as "value")

  exploded.show(false)

  spark.stop()
}
