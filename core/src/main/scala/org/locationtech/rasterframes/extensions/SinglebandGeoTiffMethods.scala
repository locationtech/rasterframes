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

import geotrellis.proj4.CRS
import geotrellis.raster.Dimensions
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.util.MethodExtensions
import geotrellis.vector.Extent
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.tiles.ProjectedRasterTile
import geotrellis.raster.Tile

trait SinglebandGeoTiffMethods extends MethodExtensions[SinglebandGeoTiff] {
  def toDF(dims: Dimensions[Int] = NOMINAL_TILE_DIMS)(implicit spark: SparkSession): DataFrame = {
    val segmentLayout = self.imageData.segmentLayout
    val re = self.rasterExtent
    val crs = self.crs

    val windows = segmentLayout.listWindows(dims.cols, dims.rows)
    val subtiles = self.crop(windows)

    val rows = for {
      (gridbounds, tile) <- subtiles.toSeq
    } yield {
      val extent = re.extentFor(gridbounds, false)
      (extent, crs, tile)
    }

    spark.createDataset(rows)(typedExpressionEncoder[(Extent, CRS, Tile)]).toDF("extent", "crs", "tile")
  }

  def toProjectedRasterTile: ProjectedRasterTile = ProjectedRasterTile(self.tile, self.extent, self.crs)
}
