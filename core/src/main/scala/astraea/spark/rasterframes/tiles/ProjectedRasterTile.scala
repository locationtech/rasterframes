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

import astraea.spark.rasterframes.ref.ProjectedRasterLike
import geotrellis.proj4.CRS
import geotrellis.raster.{ProjectedRaster, Tile}
import geotrellis.vector.{Extent, ProjectedExtent}

/**
 * A Tile that's also like a ProjectedRaster, with delayed evaluation support.
 *
 * @since 9/5/18
 */
trait ProjectedRasterTile extends DelegatingTile with ProjectedRasterLike {
  def extent: Extent
  def crs: CRS
  def projectedExtent: ProjectedExtent = ProjectedExtent(extent, crs)
  def projectedRaster: ProjectedRaster[Tile] = ProjectedRaster[Tile](this, extent, crs)
}

object ProjectedRasterTile {
  def apply(t: Tile, extent: Extent, crs: CRS): ProjectedRasterTile = ConcreteProjectedRasterTile(t, extent, crs)
  def apply(pr: ProjectedRaster[Tile]): ProjectedRasterTile = ConcreteProjectedRasterTile(pr.tile, pr.extent, pr.crs)

  case class ConcreteProjectedRasterTile(t: Tile, extent: Extent, crs: CRS) extends ProjectedRasterTile {
    def delegate: Tile = t
  }
}
