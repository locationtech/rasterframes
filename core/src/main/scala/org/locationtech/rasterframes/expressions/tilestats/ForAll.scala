package org.locationtech.rasterframes.expressions.tilestats

import geotrellis.raster.Tile
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.encoders.SparkBasicEncoders._
import org.locationtech.rasterframes.expressions.UnaryRasterFunction
import org.locationtech.rasterframes.model.TileContext
import spire.syntax.cfor.cfor

@ExpressionDescription(
  usage = "_FUNC_(tile) - Returns true if all cells in the tile are true (non-zero and not nodata).",
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
case class ForAll(child: Expression) extends UnaryRasterFunction with CodegenFallback {
  override def nodeName: String = "for_all"
  def dataType: DataType = BooleanType
  protected def eval(tile: Tile, ctx: Option[TileContext]): Any = ForAll.op(tile)
}

object ForAll {
  def apply(tile: Column): TypedColumn[Any, Boolean] = new Column(ForAll(tile.expr)).as[Boolean]

  def op(tile: Tile): Boolean = {
    cfor(0)(_ < tile.rows, _ + 1) { r =>
      cfor(0)(_ < tile.cols, _ + 1) { c =>
        if (tile.cellType.isFloatingPoint) {
          if (!isCellTrue(tile.getDouble(c, r))) return false
        }
        else {
          if (!isCellTrue(tile.get(c, r))) return false
        }
      }
    }
    true
  }
}
