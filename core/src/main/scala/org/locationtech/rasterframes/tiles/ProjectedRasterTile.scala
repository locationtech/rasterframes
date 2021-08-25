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

package org.locationtech.rasterframes.tiles

import geotrellis.proj4.CRS
import geotrellis.raster.{DelegatingTile, ProjectedRaster, Tile}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.locationtech.rasterframes.ref.ProjectedRasterLike
import org.apache.spark.sql.catalyst.DefinedByConstructorParams

/**
 * A Tile that's also like a ProjectedRaster, with delayed evaluation support.
 *
 * @since 9/5/18
 */
trait ProjectedRasterTile extends DelegatingTile with ProjectedRasterLike with DefinedByConstructorParams {
  def tile: Tile
  def extent: Extent
  def crs: CRS
  def projectedExtent: ProjectedExtent = ProjectedExtent(extent, crs)
  def projectedRaster: ProjectedRaster[Tile] = ProjectedRaster[Tile](this, extent, crs)
  def mapTile(f: Tile => Tile): ProjectedRasterTile = ProjectedRasterTile(f(this), extent, crs)
}

object ProjectedRasterTile {
  def apply(tile: Tile, extent: Extent, crs: CRS): ProjectedRasterTile = {
      val tileArg = tile
      val extentArg = extent
      val crsArg = crs
      new ProjectedRasterTile {
        def tile = tileArg
        def delegate = tileArg
        def extent = extentArg
        def crs = crsArg
      }
  }

  def unapply(prt: ProjectedRasterTile): Option[(Tile, Extent, CRS)] =
    Some((prt.tile, prt.extent, prt.crs))

  implicit val prtEncoder: ExpressionEncoder[ProjectedRasterTile] = ExpressionEncoder[ProjectedRasterTile]()
}