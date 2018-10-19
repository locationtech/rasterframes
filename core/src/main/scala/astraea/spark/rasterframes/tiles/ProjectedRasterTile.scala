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

import astraea.spark.rasterframes.tiles.ProjectedRasterTile.SourceKind.SourceKind
import geotrellis.proj4.CRS
import geotrellis.raster.{ProjectedRaster, Tile, TileLayout}
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.vector.{Extent, ProjectedExtent}

/**
 * A Tile that's also like a ProjectedRaster, with delayed evaluation support.
 *
 * @since 9/5/18
 */
trait ProjectedRasterTile extends DelegatingTile {
  def extent: Extent
  def crs: CRS
  def sourceKind: SourceKind
  def projectedExtent: ProjectedExtent = ProjectedExtent(extent, crs)
  def projectedRaster = ProjectedRaster[Tile](this, extent, crs)
//  def reproject(dest: CRS): ProjectedRasterTile =
//    if(this.crs != dest) DelayedReprojectionTile(this, dest)
//    else this
//  override def convert(ct: CellType): ProjectedRasterTile =
//    if(this.cellType != ct) DelayedConversionTile(this, ct)
//    else this
}

object ProjectedRasterTile {
  object SourceKind extends Enumeration {
    type SourceKind = Value
    val Concrete, Reference = Value
  }

  def apply(t: Tile, extent: Extent, crs: CRS): ProjectedRasterTile = ConcreteProjectedRasterTile(t, extent, crs)

  def apply(pr: ProjectedRaster[Tile]): ProjectedRasterTile = ConcreteProjectedRasterTile(pr.tile, pr.extent, pr.crs)

  private[rasterframes]
  def defaultLayout(prt: ProjectedRasterTile): LayoutDefinition =
    LayoutDefinition(prt.extent, TileLayout(1, 1, prt.cols, prt.rows))

  case class ConcreteProjectedRasterTile(t: Tile, extent: Extent, crs: CRS) extends ProjectedRasterTile {
    def delegate: Tile = t
    def sourceKind: SourceKind = SourceKind.Concrete
  }
//
//  abstract class DelayedTransformationTile(base: ProjectedRasterTile) extends ProjectedRasterTile {
//    override def extent: Extent = base.extent
//    override def crs: CRS = base.crs
//    override def cellType: CellType = base.cellType
//    override def sourceKind: SourceKind = base.sourceKind
//  }
//
//  case class DelayedReprojectionTile(base: ProjectedRasterTile, override val crs: CRS)
//    extends DelayedTransformationTile(base) {
//    protected def delegate: Tile = realized
//    override def extent: Extent = base.extent.reproject(base.crs, crs)
//    lazy val realized: Tile = base.reproject(base.extent, base.crs, crs).tile
//  }
//
//  case class DelayedConversionTile(base: ProjectedRasterTile, override val cellType: CellType)
//    extends DelayedTransformationTile(base) {
//    protected def delegate: Tile = realized
//    override def cols: Int = base.cols
//    override def rows: Int = base.rows
//    lazy val realized: Tile = base.convert(cellType)
//  }
}
