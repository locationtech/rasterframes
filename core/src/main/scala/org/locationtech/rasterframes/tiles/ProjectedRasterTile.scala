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
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster.{DelegatingTile, ProjectedRaster, Tile}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.rf.TileUDT
import org.apache.spark.sql.types.{StructField, StructType}
import org.locationtech.rasterframes.{CrsType, TileType}
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.encoders.{CatalystSerializer, CatalystSerializerEncoder}
import org.locationtech.rasterframes.model.TileContext
import org.locationtech.rasterframes.ref.ProjectedRasterLike
import org.locationtech.rasterframes.ref.RasterRef.RasterRefTile

/**
 * A Tile that's also like a ProjectedRaster, with delayed evaluation support.
 *
 * @since 9/5/18
 */
case class ProjectedRasterTile(tile: Tile, extent: Extent, crs: CRS) extends DelegatingTile with ProjectedRasterLike {
  def delegate: Tile = tile
  def projectedExtent: ProjectedExtent = ProjectedExtent(extent, crs)
  def projectedRaster: ProjectedRaster[Tile] = ProjectedRaster[Tile](this, extent, crs)
  def mapTile(f: Tile => Tile): ProjectedRasterTile = ProjectedRasterTile(f(this), extent, crs)
}

object ProjectedRasterTile {
  def apply(pr: ProjectedRaster[Tile]): ProjectedRasterTile =
    ProjectedRasterTile(pr.tile, pr.extent, pr.crs)
  def apply(tiff: SinglebandGeoTiff): ProjectedRasterTile =
    ProjectedRasterTile(tiff.tile, tiff.extent, tiff.crs)
  def apply(tile: RasterRefTile): ProjectedRasterTile =
    ProjectedRasterTile(tile, tile.rr.extent, tile.rr.crs)

  implicit val serializer: CatalystSerializer[ProjectedRasterTile] = new CatalystSerializer[ProjectedRasterTile] {
    override val schema: StructType = StructType(Seq(
      StructField("tile", TileType, false),
      StructField("extent", schemaOf[Extent], false),
      StructField("crs", CrsType, false)))

    override protected def to[R](t: ProjectedRasterTile, io: CatalystIO[R]): R = io.create(
      io.to[Tile](t)(TileUDT.tileSerializer),
      io.to[Extent](t.extent),
      io.to[CRS](t.crs)
    )

    override protected def from[R](t: R, io: CatalystIO[R]): ProjectedRasterTile = {
      val tile = io.get[Tile](t, ordinal = 0)(TileUDT.tileSerializer)

      tile match {
        case r: RasterRefTile =>
          ProjectedRasterTile(r, r.rr.extent, r.rr.crs)
        case _ =>
          val extent = io.get[Extent](t, ordinal = 1)
          val crs = io.get[CRS](t, ordinal = 2)
          val resolved = tile match {
            case i: InternalRowTile => i.toArrayTile()
            case o => o
          }
          ProjectedRasterTile(resolved, extent, crs)
      }
    }
  }

  implicit val prtEncoder: ExpressionEncoder[ProjectedRasterTile] = ExpressionEncoder[ProjectedRasterTile]()
}
