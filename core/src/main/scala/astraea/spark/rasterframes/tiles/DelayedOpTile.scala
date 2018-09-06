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

import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, Tile}
import geotrellis.vector.Extent

/**
 *
 *
 * @since 9/6/18
 */
trait DelayedOpTile extends ProjectedRasterTile

object DelayedOpTile {
  case class DelayedReadTile(reader: () ⇒ Tile, extent: Extent, crs: CRS) extends DelayedOpTile {
    override protected def delegate: Tile = reader()
  }

  case class DelayedReprojectionTile(base: ProjectedRasterTile, crs: CRS) extends DelayedOpTile {
    val extent: Extent = base.extent.reproject(base.crs, crs)
    lazy val realized = {
      val raster = base.reproject(base.extent, base.crs, crs)
      raster.tile
    }
    override protected def delegate: Tile = realized
  }
}
