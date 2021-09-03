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

package org.locationtech.rasterframes.model

import geotrellis.proj4.CRS
import geotrellis.raster.Tile
import geotrellis.vector.{Extent, ProjectedExtent}
import org.locationtech.rasterframes.tiles.ProjectedRasterTile

case class TileContext(extent: Extent, crs: CRS) {
  def toProjectRasterTile(t: Tile): ProjectedRasterTile = ProjectedRasterTile(t, extent, crs)
  def projectedExtent: ProjectedExtent = ProjectedExtent(extent, crs)
}
object TileContext {
  def apply(prt: ProjectedRasterTile): TileContext = new TileContext(prt.extent, prt.crs)
  def unapply(tile: Tile): Option[(Extent, CRS)] = tile match {
    case prt: ProjectedRasterTile => Some((prt.extent, prt.crs))
    case _ => None
  }
}
