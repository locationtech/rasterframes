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

import astraea.spark.rasterframes.encoders.CatalystSerializer.CatalystIO
import astraea.spark.rasterframes.encoders.{CatalystSerializer, CatalystSerializerEncoder}
import astraea.spark.rasterframes.tiles.ProjectedRasterTile
import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, GridBounds, Tile}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.rf.RasterSourceUDT
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import CatalystSerializer._

/**
 * A delayed-read projected raster implementation.
 *
 * @since 8/21/18
 */
case class RasterRef(source: RasterSource, bandIndex: Int, subextent: Option[Extent])
  extends ProjectedRasterLike {
  def crs: CRS = source.crs
  def extent: Extent = subextent.getOrElse(source.extent)
  def projectedExtent: ProjectedExtent = ProjectedExtent(extent, crs)
  def cols: Int = grid.width
  def rows: Int = grid.height
  def cellType: CellType = source.cellType
  def tile: ProjectedRasterTile = ProjectedRasterTile(realizedTile, extent, crs)

  protected lazy val grid: GridBounds = source.rasterExtent.gridBoundsFor(extent)
  protected def srcExtent: Extent = extent

  protected lazy val realizedTile: Tile = {
    RasterRef.log.trace(s"Fetching $srcExtent from band $bandIndex of $source")
    source.read(srcExtent, Seq(bandIndex)).tile.band(0)
  }
}

object RasterRef extends LazyLogging {
  private val log = logger

  case class RasterRefTile(rr: RasterRef) extends ProjectedRasterTile {
    val extent: Extent = rr.extent
    val crs: CRS = rr.crs
    override val cellType = rr.cellType

    override val cols: Int = rr.cols
    override val rows: Int = rr.rows

    protected def delegate: Tile = rr.realizedTile
    // NB: This saves us from stack overflow exception
    override def convert(ct: CellType): ProjectedRasterTile =
      ProjectedRasterTile(rr.realizedTile.convert(ct), extent, crs)
  }

  implicit val rasterRefSerializer: CatalystSerializer[RasterRef] = new CatalystSerializer[RasterRef] {
    val rsType = new RasterSourceUDT()
    override def schema: StructType = StructType(Seq(
      StructField("source", rsType, false),
      StructField("bandIndex", IntegerType, false),
      StructField("subextent", schemaOf[Extent], true)
    ))

    override def to[R](t: RasterRef, io: CatalystIO[R]): R = io.create(
      io.to(t.source)(RasterSourceUDT.rasterSourceSerializer),
      t.bandIndex,
      t.subextent.map(io.to[Extent]).orNull
    )

    override def from[R](row: R, io: CatalystIO[R]): RasterRef = RasterRef(
      io.get[RasterSource](row, 0)(RasterSourceUDT.rasterSourceSerializer),
      io.getInt(row, 1),
      if (io.isNullAt(row, 2)) None
      else Option(io.get[Extent](row, 2))
    )
  }

  implicit def rrEncoder: ExpressionEncoder[RasterRef] = CatalystSerializerEncoder[RasterRef](true)
}
