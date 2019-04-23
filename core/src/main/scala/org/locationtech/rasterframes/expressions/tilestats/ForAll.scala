package org.locationtech.rasterframes.expressions.tilestats

import geotrellis.raster.{Tile, isData}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.rasterframes.expressions.UnaryRasterOp
import org.locationtech.rasterframes.model.TileContext

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
case class ForAll(child: Expression) extends UnaryRasterOp with CodegenFallback {
  override def nodeName: String = "for_all"
  override def dataType: DataType = BooleanType
  override protected def eval(tile: Tile, ctx: Option[TileContext]): Any = ForAll.op(tile)

}

object ForAll{

  def apply(tile: Column): TypedColumn[Any, Boolean] = new Column(ForAll(tile.expr)).as[Boolean]

  def op(tile: Tile): Boolean = {

    def doubleValueIs(d: Double): Boolean =  isData(d) & d != 0.0
    def intValueIs(i: Int): Boolean =  isData(i) & i != 0

    var (c, r) = (0, 0)
    while (r < tile.rows) {
      while(c < tile.cols) {
        if(tile.cellType.isFloatingPoint) { if(! doubleValueIs(tile.getDouble(c, r))) return false }
        else { if(! intValueIs(tile.get(c, r))) return false }
        c += 1
      }
      c = 0; r += 1
    }

    true
  }


}