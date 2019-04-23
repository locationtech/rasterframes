package astraea.spark.rasterframes.expressions.tilestats

import astraea.spark.rasterframes.expressions.UnaryRasterOp
import astraea.spark.rasterframes.model.TileContext
import geotrellis.raster.{Tile, isData}
import org.apache.spark.sql.{Column, TypedColumn}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

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
  import astraea.spark.rasterframes.encoders.StandardEncoders.PrimitiveEncoders.boolEnc

  def apply(tile: Column): TypedColumn[Any, Boolean] = new Column(Exists(tile.expr)).as[Boolean]

  def op(tile: Tile): Boolean = {

    def doubleValueIs(d: Double): Boolean =  isData(d) & d != 0.0
    def intValueIs(i: Int): Boolean =  isData(i) & i != 0

    var (c, r) = (0, 0)
    while (r < tile.rows) {
      while(c < tile.cols) {
        if(tile.cellType.isFloatingPoint) { if(doubleValueIs(tile.getDouble(c, r))) return true }
        else { if(intValueIs(tile.get(c, r))) return true }
        c += 1
      }
      c = 0; r += 1
    }

    false
  }


}