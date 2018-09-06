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

import astraea.spark.rasterframes.tiles.DelayedOpTile.DelayedReadTile
import astraea.spark.rasterframes.tiles.ProjectedRasterTile
import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, GridBounds, Tile}
import geotrellis.vector.Extent

/**
 * A delayed-read projected raster implementation.
 *
 * @since 8/21/18
 */
trait RasterRef {
  def source: RasterSource
  def crs: CRS = source.crs
  def extent: Extent
  def cols: Int = grid.width
  def rows: Int = grid.height
  def cellType: CellType = source.cellType
  def tile: ProjectedRasterTile = DelayedReadTile(() ⇒ realizedTile, extent, crs)

  protected def grid: GridBounds = source.rasterExtent.gridBoundsFor(extent)
  protected def srcExtent: Extent = extent
  protected lazy val realizedTile: Tile = {
    require(source.bandCount == 1, "Expected singleband tile")
    RasterRef.log.debug(s"Fetching $srcExtent from $source")
    source.read(srcExtent).left.get.tile
  }
}

object RasterRef extends LazyLogging {
  private val log = logger
  /** Constructor for when data extent cover whole raster. */
  def apply(source: RasterSource): RasterRef = FullRasterRef(source)
  /** Constructor for when data extent covers a subpart of the raster */
  def apply(source: RasterSource, subextent: Extent): RasterRef = SubRasterRef(source, subextent)

  /** Splits this tile into smaller tiles based on the reported
   * internal structure of the backing format. May return a single item.*/
  private[rasterframes] def tileToNative(ref: RasterRef): Seq[RasterRef] = {
    val ex = ref.extent
    ref.source.nativeTiling
      .filter(_ intersects ex)
      .map(e ⇒ SubRasterRef(ref.source, e))
  }

  case class FullRasterRef(source: RasterSource) extends RasterRef {
    val extent: Extent = source.extent
  }

  case class SubRasterRef(source: RasterSource, subextent: Extent) extends RasterRef {
    val extent: Extent = subextent
  }
}
