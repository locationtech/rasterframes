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

package org.locationtech.rasterframes.extensions

import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.util.MethodExtensions
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.model.TileDimensions
import org.locationtech.rasterframes.tiles.ProjectedRasterTile

trait SinglebandGeoTiffMethods extends MethodExtensions[SinglebandGeoTiff] {
  def toDF(dims: TileDimensions = NOMINAL_TILE_DIMS)(implicit spark: SparkSession): DataFrame = {

    val segmentLayout = self.imageData.segmentLayout
    val re = self.rasterExtent
    val crs = self.crs

    val windows = segmentLayout.listWindows(256)
    val subtiles = self.crop(windows)

    val rows = for {
      (gridbounds, tile) ← subtiles.toSeq
    } yield {
      val extent = re.extentFor(gridbounds, false)
      ProjectedRasterTile(tile, extent, crs).toRow
    }

    spark.createDataFrame(spark.sparkContext.makeRDD(rows, 1), schemaOf[ProjectedRasterTile])
      .select("tileContext.extent", "tileContext.crs", "tile")
  }
}
