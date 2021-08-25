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

package org.locationtech.rasterframes.ref

import com.typesafe.scalalogging.LazyLogging
import frameless.TypedExpressionEncoder
import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, GridBounds, Tile}
import geotrellis.vector.Extent
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.locationtech.rasterframes.RasterSourceType
import org.locationtech.rasterframes.tiles.ProjectedRasterTile

/**
 * A delayed-read projected raster implementation.
 *
 * @since 8/21/18
 */
case class RasterRef(source: RFRasterSource, bandIndex: Int, subextent: Option[Extent], subgrid: Option[Subgrid])
  extends  ProjectedRasterTile {
  def tile: Tile = this
  def extent: Extent = subextent.getOrElse(source.extent)
  def crs: CRS = source.crs
  def delegate = realizedTile

  override def cols: Int = grid.width
  override def rows: Int = grid.height
  override def cellType: CellType = source.cellType

  protected lazy val grid: GridBounds[Int] =
    subgrid.map(_.toGridBounds).getOrElse(source.rasterExtent.gridBoundsFor(extent, true))

  lazy val realizedTile: Tile = {
    RasterRef.log.trace(s"Fetching $extent ($grid) from band $bandIndex of $source")
    source.read(grid, Seq(bandIndex)).tile.band(0)
  }

  override def toString: String = s"RasterRef($source,$bandIndex,$cellType)"
}

object RasterRef extends LazyLogging {
  private val log = logger

  def apply(source: RFRasterSource, bandIndex: Int): RasterRef =
    RasterRef(source, bandIndex, None, None)

  def apply(source: RFRasterSource, bandIndex: Int, subextent: Extent, subgrid: GridBounds[Int]): RasterRef =
    RasterRef(source, bandIndex, Some(subextent), Some(Subgrid(subgrid)))


  implicit val rrEncoder: ExpressionEncoder[RasterRef] = {
    import org.locationtech.rasterframes.encoders.StandardEncoders._
    TypedExpressionEncoder[RasterRef].asInstanceOf[ExpressionEncoder[RasterRef]]
  }
}