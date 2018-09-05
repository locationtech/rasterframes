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

import astraea.spark.rasterframes.ref.RasterSource
import geotrellis.raster.{CellType, ProjectedRaster, Raster, Tile}
import geotrellis.vector.Extent

/**
 * A delayed-read Tile implementation.
 *
 * @since 8/21/18
 */
case class DelayedReadTile(source: RasterSource, extent: Option[Extent]) extends DelegatingTile
  with MaybeProjected {
  private lazy val realized: Tile = {
    require(source.bandCount == 1, "Expected singleband tile")
    logger.debug(s"Fetching $extent from $source")
    source.read(extent.getOrElse(source.extent)).left.get.tile
  }
  private def subgrid =
    source.rasterExtent.gridBoundsFor(extent.getOrElse(source.extent))
  protected override def delegate: Tile = realized

  override def cellType: CellType = source.cellType

  override def cols: Int = subgrid.width
  override def rows: Int = subgrid.height

  /** Splits this tile into smaller tiles based on the reported
   * internal structure of the backing format. May return a single item.*/
  def tileToNative: Seq[DelayedReadTile] = {
    val hereExtent = extent.getOrElse(source.extent)
    source.nativeTiling
      .filter(_ intersects hereExtent)
      .map(e ⇒ DelayedReadTile(source, Some(e)))
  }

  override def toString: String = {
    s"${getClass.getSimpleName}($extent,$source)"
  }

  override def projected: Option[ProjectedRaster[Tile]] = Some(
    ProjectedRaster(Raster(this, extent.getOrElse(source.extent)), source.crs)
  )
}

object DelayedReadTile {
  /** Constructor for when data extent cover whole raster. */
  def apply(source: RasterSource): DelayedReadTile =
    new DelayedReadTile(source, None)
}
