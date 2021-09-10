package org.locationtech.rasterframes.expressions

import geotrellis.raster.Tile
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.encoders._
import org.locationtech.rasterframes.model.TileContext
import org.locationtech.rasterframes.tiles.ProjectedRasterTile

trait RasterResult { self: Expression =>
  private lazy val tileSer: Tile => InternalRow = tileUDT.serialize
  private lazy val prtSer: ProjectedRasterTile => InternalRow = cachedSerializer[ProjectedRasterTile]

  def toInternalRow(result: Tile, tileContext: Option[TileContext] = None): InternalRow =
    tileContext.fold
      {tileSer(result)}
      {ctx => prtSer(ProjectedRasterTile(result, ctx.extent, ctx.crs))}

  def toInternalRow(result: ProjectedRasterTile): InternalRow = prtSer(result)
}
