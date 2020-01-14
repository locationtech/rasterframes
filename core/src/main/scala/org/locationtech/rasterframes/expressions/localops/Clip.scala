package org.locationtech.rasterframes.expressions.localops

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, TernaryExpression}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.rf.TileUDT
import org.apache.spark.sql.types.DataType
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.expressions.DynamicExtractors._
import org.locationtech.rasterframes.expressions.row

@ExpressionDescription(
  usage = "_FUNC_(tile, min, max) - Return the tile with its values clipped to a range defined by min and max," +
            " doing so cellwise if min or max are tile type",
  arguments = """
      Arguments:
        * tile - the tile to operate on
        * min - scalar or tile setting the minimum value for each cell
        * max - scalar or tile setting the maximum value for each cell"""
)
case class Clip(left: Expression, middle: Expression, right: Expression)
  extends TernaryExpression with CodegenFallback with Serializable {
  override def dataType: DataType = left.dataType

  override def children: Seq[Expression] = Seq(left, middle, right)

  override val nodeName = "rf_clip"

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!tileExtractor.isDefinedAt(left.dataType)) {
      TypeCheckFailure(s"Input type '${left.dataType}' does not conform to a Tile type")
    } else if (!tileExtractor.isDefinedAt(middle.dataType) && !numberArgExtractor.isDefinedAt(middle.dataType)) {
      TypeCheckFailure(s"Input type '${middle.dataType}' does not conform to a Tile or numeric type")
    } else if (!tileExtractor.isDefinedAt(right.dataType) && !numberArgExtractor.isDefinedAt(right.dataType)) {
      TypeCheckFailure(s"Input type '${right.dataType}' does not conform to a Tile or numeric type")
    }
    else TypeCheckSuccess
  }

  override protected def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
    implicit val tileSer = TileUDT.tileSerializer
    val (targetTile, targetCtx) = tileExtractor(left.dataType)(row(input1))
    val minVal = tileOrNumberExtractor(middle.dataType)(input2)
    val maxVal = tileOrNumberExtractor(right.dataType)(input3)

    val result = (minVal, maxVal) match {
      case (mn: TileArg, mx: TileArg) ⇒ targetTile.localMin(mx.tile).localMax(mn.tile)
      case (mn: TileArg, mx: IntegerArg) ⇒ targetTile.localMin(mx.value).localMax(mn.tile)
      case (mn: TileArg, mx: DoubleArg) ⇒ targetTile.localMin(mx.value).localMax(mn.tile)
      case (mn: IntegerArg, mx: TileArg) ⇒ targetTile.localMin(mx.tile).localMax(mn.value)
      case (mn: IntegerArg, mx: IntegerArg) ⇒ targetTile.localMin(mx.value).localMax(mn.value)
      case (mn: IntegerArg, mx: DoubleArg) ⇒ targetTile.localMin(mx.value).localMax(mn.value)
      case (mn: DoubleArg, mx: TileArg) ⇒ targetTile.localMin(mx.tile).localMax(mn.value)
      case (mn: DoubleArg, mx: IntegerArg) ⇒ targetTile.localMin(mx.value).localMax(mn.value)
      case (mn: DoubleArg, mx: DoubleArg) ⇒ targetTile.localMin(mx.value).localMax(mn.value)
    }

    targetCtx match {
      case Some(ctx) => ctx.toProjectRasterTile(result).toInternalRow
      case None => result.toInternalRow
    }
  }

}
object Clip {
  def apply(tile: Column, min: Column, max: Column): Column = new Column(Clip(tile.expr, min.expr, max.expr))
  def apply[N: Numeric](tile: Column, min: N, max: Column): Column = new Column(Clip(tile.expr, lit(min).expr, max.expr))
  def apply[N: Numeric](tile: Column, min: Column, max: N): Column = new Column(Clip(tile.expr, min.expr, lit(max).expr))
  def apply[N: Numeric](tile: Column, min: N, max: N): Column = new Column(Clip(tile.expr, lit(min).expr, lit(max).expr))

}
