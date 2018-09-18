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

import astraea.spark.rasterframes.ref.RasterRef.RasterRefTile
import astraea.spark.rasterframes.tiles.ProjectedRasterTile
import astraea.spark.rasterframes.tiles.ProjectedRasterTile.SourceKind
import astraea.spark.rasterframes.tiles.ProjectedRasterTile.SourceKind.SourceKind
import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, GridBounds, Tile, TileLayout}
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.vector.{Extent, ProjectedExtent}

/**
 * A delayed-read projected raster implementation.
 *
 * @since 8/21/18
 */
trait RasterRef {
  def source: RasterSource
  def crs: CRS = source.crs
  def extent: Extent
  def projectedExtent: ProjectedExtent = ProjectedExtent(extent, crs)
  def cols: Int = grid.width
  def rows: Int = grid.height
  def cellType: CellType = source.cellType
  def tile: ProjectedRasterTile = RasterRefTile(this)

  protected def grid: GridBounds = source.rasterExtent.gridBoundsFor(extent)
  protected def srcExtent: Extent = extent
  @transient
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

  private[rasterframes]
  def defaultLayout(rr: RasterRef): LayoutDefinition =
    LayoutDefinition(rr.extent, rr.source.nativeLayout
      .getOrElse(TileLayout(1, 1, rr.cols, rr.rows))
    )

  case class FullRasterRef(source: RasterSource) extends RasterRef {
    val extent: Extent = source.extent
  }

  case class SubRasterRef(source: RasterSource, subextent: Extent) extends RasterRef {
    val extent: Extent = subextent
  }

  case class RasterRefTile(rr: RasterRef) extends ProjectedRasterTile {
    def extent: Extent = rr.extent
    def crs: CRS = rr.crs

    override def cols: Int = rr.cols
    override def rows: Int = rr.rows

    protected def delegate: Tile = rr.realizedTile
    def sourceKind: SourceKind = SourceKind.Reference
    // NB: This saves us from stack overflow exception
    override def convert(ct: CellType): ProjectedRasterTile =
      ProjectedRasterTile(rr.realizedTile.convert(ct), extent, crs)
  }
}
