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

import geotrellis.raster.mapalgebra.focal.Square
import org.apache.spark.sql._
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.datasource.raster._
import org.locationtech.rasterframes.tiles.ProjectedRasterTile

object BufferTiles extends App {

  implicit val spark =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("RasterFrames")
      .withKryoSerialization
      .getOrCreate()
      .withRasterFrames

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val example = "https://raw.githubusercontent.com/locationtech/rasterframes/develop/core/src/test/resources/LC08_B7_Memphis_COG.tiff"

  val tile =
    spark
      .read
      .raster
      .from(example)
      .withBufferSize(1)
      .withTileDimensions(100, 100)
      .load()
      .limit(1)
      .select($"proj_raster")
      .select(rf_focal_max($"proj_raster", Square(1)))
      // .select(rf_aspect($"proj_raster"))
      // .select(rf_hillshade($"proj_raster", 315, 45, 1))
      .as[Option[ProjectedRasterTile]]
      // .show(false)
      .first()

  // tile.get.renderPng().write("/tmp/hillshade-buffered.png")
  // tile.get.renderPng().write("/tmp/hillshade-nobuffered.png")

  // spark.stop()
}
