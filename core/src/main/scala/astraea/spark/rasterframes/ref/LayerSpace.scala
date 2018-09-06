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

import geotrellis.proj4.CRS
import geotrellis.raster._
import geotrellis.raster.resample.ResampleMethod
import geotrellis.spark.{SpatialKey, _}
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.vector._


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
  def project(raster: RasterRef): Seq[RasterRef] = {
    val inExtent = raster.extent.reproject(raster.crs, crs)
    val bounds: TileBounds = layout.mapTransform(inExtent)
    bounds.coordsIter.map { case (col, row) ⇒
      val outKey = SpatialKey(col, row)
      val outExtent = layout.mapTransform.keyToExtent(outKey)
      //RasterRef(raster.source, ProjectedExtent(outExtent, crs))
      ???
    }.toSeq
  }
}

object LayerSpace {
  def from(rr: RasterRef): LayerSpace = new LayerSpace(rr.crs, rr.cellType,
    LayoutDefinition(rr.extent, rr.source.nativeLayout
      .getOrElse(TileLayout(1, 1, rr.cols, rr.rows))
    )
  )
}
