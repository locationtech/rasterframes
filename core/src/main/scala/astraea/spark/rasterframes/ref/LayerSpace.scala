/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea, Inc.
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

package astraea.spark.rasterframes.ref

import astraea.spark.rasterframes.tiles.ProjectedRasterTile
import geotrellis.proj4.CRS
import geotrellis.raster._
import geotrellis.raster.resample.ResampleMethod
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.spark.{SpatialKey, _}


/**
 * NB: This package is only a temporary home for this.
 *
 * @since 9/5/18
 */
case class LayerSpace(
  crs: CRS,
  cellType: CellType,
  layout: LayoutDefinition,
  resampleMethod: ResampleMethod = ResampleMethod.DEFAULT
) {

  def reproject(dest: CRS): LayerSpace = {
    copy(
      crs = dest,
      layout = layout.copy(extent = layout.extent.reproject(crs, dest))
    )
  }

  def asTileLayerMetadata: TileLayerMetadata[SpatialKey] = {
    val bounds = KeyBounds(
      SpatialKey(0, 0),
      SpatialKey(layout.layoutCols - 1, layout.layoutRows - 1)
    )
    TileLayerMetadata(cellType, layout, layout.extent, crs, bounds)
  }
}

object LayerSpace {

  def from(rs: RasterSource): LayerSpace = new LayerSpace(
    rs.crs, rs.cellType, LayoutDefinition(rs.extent, rs.nativeLayout
      .getOrElse(TileLayout(1, 1, rs.cols, rs.rows))
    )
  )

  def from(rr: RasterRef): LayerSpace = new LayerSpace(
    rr.crs, rr.cellType, RasterRef.defaultLayout(rr)
  )

  def from(prt: ProjectedRasterTile): LayerSpace = new LayerSpace(
    prt.crs, prt.cellType, ProjectedRasterTile.defaultLayout(prt)
  )

}
