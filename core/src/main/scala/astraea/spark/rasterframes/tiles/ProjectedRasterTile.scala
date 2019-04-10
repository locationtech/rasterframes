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

import astraea.spark.rasterframes.TileType
import astraea.spark.rasterframes.encoders.CatalystSerializer._
import astraea.spark.rasterframes.encoders.{CatalystSerializer, CatalystSerializerEncoder}
import astraea.spark.rasterframes.model.TileContext
import astraea.spark.rasterframes.ref.ProjectedRasterLike
import astraea.spark.rasterframes.ref.RasterRef.RasterRefTile
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster.{CellType, ProjectedRaster, Tile}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.rf.TileUDT
import org.apache.spark.sql.types.{StructField, StructType}

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
  def apply(t: Tile, extent: Extent, crs: CRS): ProjectedRasterTile =
    ConcreteProjectedRasterTile(t, extent, crs)
  def apply(pr: ProjectedRaster[Tile]): ProjectedRasterTile =
    ConcreteProjectedRasterTile(pr.tile, pr.extent, pr.crs)
  def apply(tiff: SinglebandGeoTiff): ProjectedRasterTile =
    ConcreteProjectedRasterTile(tiff.tile, tiff.extent, tiff.crs)

  case class ConcreteProjectedRasterTile(t: Tile, extent: Extent, crs: CRS)
      extends ProjectedRasterTile {
    def delegate: Tile = t
    override def convert(cellType: CellType): Tile =
      ConcreteProjectedRasterTile(t.convert(cellType), extent, crs)
  }

  implicit val serializer: CatalystSerializer[ProjectedRasterTile] = new CatalystSerializer[ProjectedRasterTile] {
    override def schema: StructType = StructType(Seq(
      StructField("tile_context", schemaOf[TileContext], false),
      StructField("tile", TileType, false))
    )

    override protected def to[R](t: ProjectedRasterTile, io: CatalystIO[R]): R = io.create(
      io.to(TileContext(t.extent, t.crs)),
      io.to[Tile](t)(TileUDT.tileSerializer)
    )

    override protected def from[R](t: R, io: CatalystIO[R]): ProjectedRasterTile = {
      val tile = io.get[Tile](t, 1)(TileUDT.tileSerializer)
      tile match {
        case r: RasterRefTile => r
        case _ =>
          val ctx = io.get[TileContext](t, 0)
          val resolved = tile match {
            case i: InternalRowTile => i.toArrayTile()
            case o => o
          }
          ProjectedRasterTile(resolved, ctx.extent, ctx.crs)
      }
    }
  }

  implicit val prtEncoder: ExpressionEncoder[ProjectedRasterTile] = CatalystSerializerEncoder[ProjectedRasterTile](true)
}
