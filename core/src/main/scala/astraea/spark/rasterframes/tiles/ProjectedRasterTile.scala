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

package astraea.spark.rasterframes.tiles

import astraea.spark.rasterframes.tiles.DelayedOpTile.DelayedReprojectionTile
import geotrellis.proj4.CRS
import geotrellis.raster.{ProjectedRaster, Tile}
import geotrellis.vector.{Extent, ProjectedExtent}

/**
 * A Tile that's also like a ProjectedRaster
 *
 * @since 9/5/18
 */
trait ProjectedRasterTile extends DelegatingTile {
  val extent: Extent
  val crs: CRS
  val projectedExtent = ProjectedExtent(extent, crs)
  def reproject(dest: CRS): ProjectedRasterTile = DelayedReprojectionTile(this, dest)
}

object ProjectedRasterTile {
  def apply(pr: ProjectedRaster[Tile]) = new ProjectedRasterTile {
    protected def delegate: Tile = pr.tile
    override val extent: Extent = pr.extent
    override val crs: CRS = pr.crs
  }
}
