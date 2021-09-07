package org.locationtech.rasterframes.expressions.tilestats

import geotrellis.raster.Tile
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.rasterframes.isCellTrue
import org.locationtech.rasterframes.expressions.UnaryRasterOp
import org.locationtech.rasterframes.model.TileContext
import spire.syntax.cfor.cfor

@ExpressionDescription(
  usage = "_FUNC_(tile) - Returns true if any cells in the tile are true (non-zero and not nodata).",
  arguments =
    """
    Arguments:
       * tile - tile to check
    """,
  examples =
    """
    > SELECT _FUNC_(tile);
       true
    """
)
case class Exists(child: Expression) extends UnaryRasterOp with CodegenFallback {
  override def nodeName: String = "exists"
  override def dataType: DataType = BooleanType
  override protected def eval(tile: Tile, ctx: Option[TileContext]): Any = Exists.op(tile)

}

object Exists{
  import org.locationtech.rasterframes.encoders.StandardEncoders.PrimitiveEncoders.boolEnc

  def apply(tile: Column): TypedColumn[Any, Boolean] = new Column(Exists(tile.expr)).as[Boolean]

  def op(tile: Tile): Boolean = {
    cfor(0)(_ < tile.rows, _ + 1) { r =>
      cfor(0)(_ < tile.cols, _ + 1) { c =>
        if(tile.cellType.isFloatingPoint) { if(isCellTrue(tile.getDouble(c, r))) return true }
        else { if(isCellTrue(tile.get(c, r))) return true }
      }
    }
    false
  }
}