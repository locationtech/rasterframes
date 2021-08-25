package org.locationtech.rasterframes.expressions

import geotrellis.raster.Tile
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.locationtech.rasterframes.TileType
import org.locationtech.rasterframes.model.TileContext
import org.locationtech.rasterframes.tiles.ProjectedRasterTile

trait RasterResult { self: Expression =>
  private lazy val tileSer: Tile => InternalRow = TileType.serialize _
  private lazy val prtSer: ProjectedRasterTile => InternalRow = ProjectedRasterTile.prtEncoder.createSerializer()

  def toInternalRow(result: Tile, tileContext: Option[TileContext] = None): InternalRow = {
    tileContext.fold
      {tileSer(result)}
      {ctx => prtSer(ProjectedRasterTile(result, ctx.extent, ctx.crs))}
  }

  def toInternalRow(result: ProjectedRasterTile): InternalRow = {
    prtSer(result)
  }
}
