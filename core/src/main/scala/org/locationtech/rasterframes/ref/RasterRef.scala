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
import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, GridBounds, Tile}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.rf.RasterSourceUDT
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.locationtech.rasterframes.encoders.CatalystSerializer.{CatalystIO, _}
import org.locationtech.rasterframes.encoders.{CatalystSerializer, CatalystSerializerEncoder}
import org.locationtech.rasterframes.ref.RasterRef.RasterRefTile
import org.locationtech.rasterframes.tiles.ProjectedRasterTile

/**
 * A delayed-read projected raster implementation.
 *
 * @since 8/21/18
 */
case class RasterRef(source: RasterSource, bandIndex: Int, subextent: Option[Extent], subgrid: Option[GridBounds])
  extends ProjectedRasterLike {
  def crs: CRS = source.crs
  def extent: Extent = subextent.getOrElse(source.extent)
  def projectedExtent: ProjectedExtent = ProjectedExtent(extent, crs)
  def cols: Int = grid.width
  def rows: Int = grid.height
  def cellType: CellType = source.cellType
  def tile: ProjectedRasterTile = RasterRefTile(this)

  protected lazy val grid: GridBounds =
    subgrid.getOrElse(source.rasterExtent.gridBoundsFor(extent, true))

  protected lazy val realizedTile: Tile = {
    RasterRef.log.trace(s"Fetching $extent ($grid) from band $bandIndex of $source")
    source.read(grid, Seq(bandIndex)).tile.band(0)
  }
}

object RasterRef extends LazyLogging {
  private val log = logger

  case class RasterRefTile(rr: RasterRef) extends ProjectedRasterTile {
    def extent: Extent = rr.extent
    def crs: CRS = rr.crs
    override def cellType = rr.cellType

    override def cols: Int = rr.cols
    override def rows: Int = rr.rows

    protected def delegate: Tile = rr.realizedTile
    // NB: This saves us from stack overflow exception
    override def convert(ct: CellType): ProjectedRasterTile =
      ProjectedRasterTile(rr.realizedTile.convert(ct), extent, crs)
  }

  implicit val rasterRefSerializer: CatalystSerializer[RasterRef] = new CatalystSerializer[RasterRef] {
    val rsType = new RasterSourceUDT()
    override val schema: StructType = StructType(Seq(
      StructField("source", rsType.sqlType, false),
      StructField("bandIndex", IntegerType, false),
      StructField("subextent", schemaOf[Extent], true),
      StructField("subgrid", schemaOf[GridBounds], true)
    ))

    override def to[R](t: RasterRef, io: CatalystIO[R]): R = io.create(
      io.to(t.source)(RasterSourceUDT.rasterSourceSerializer),
      t.bandIndex,
      t.subextent.map(io.to[Extent]).orNull,
      t.subgrid.map(io.to[GridBounds]).orNull
    )

    override def from[R](row: R, io: CatalystIO[R]): RasterRef = RasterRef(
      io.get[RasterSource](row, 0)(RasterSourceUDT.rasterSourceSerializer),
      io.getInt(row, 1),
      if (io.isNullAt(row, 2)) None
      else Option(io.get[Extent](row, 2)),
      if (io.isNullAt(row, 3)) None
      else Option(io.get[GridBounds](row, 3))
    )
  }

  implicit def rrEncoder: ExpressionEncoder[RasterRef] = CatalystSerializerEncoder[RasterRef](true)
}
