package org.locationtech.rasterframes.expressions.localops

import com.typesafe.scalalogging.Logger
import geotrellis.raster.Tile
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, TernaryExpression}
import org.apache.spark.sql.types.DataType
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.expressions.DynamicExtractors._
import org.locationtech.rasterframes.expressions.{RasterResult, row}
import org.slf4j.LoggerFactory

@ExpressionDescription(
  usage = "_FUNC_(tile, min, max) - Return a tile with cell values chosen from `x` or `y` depending on `condition`. Operates cell-wise in a similar fashion to Spark SQL `when` and `otherwise`.",
  arguments = """
      Arguments:
        * condition - the tile of values to evaluate as true
        * x - tile with cell values to return if condition is true
        * y - tile with cell values to return if condition is false"""
)
case class Where(left: Expression, middle: Expression, right: Expression)
  extends TernaryExpression with RasterResult with CodegenFallback with Serializable {

  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  override def dataType: DataType = middle.dataType

  override def children: Seq[Expression] = Seq(left, middle, right)

  override val nodeName = "rf_where"

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!tileExtractor.isDefinedAt(left.dataType)) {
      TypeCheckFailure(s"Input type '${left.dataType}' does not conform to a Tile type")
    } else if (!tileExtractor.isDefinedAt(middle.dataType)) {
      TypeCheckFailure(s"Input type '${middle.dataType}' does not conform to a Tile type")
    } else if (!tileExtractor.isDefinedAt(right.dataType)) {
      TypeCheckFailure(s"Input type '${right.dataType}' does not conform to a Tile type")
    }
    else TypeCheckSuccess
  }

  override protected def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
    val (conditionTile, conditionCtx) = tileExtractor(left.dataType)(row(input1))
    val (xTile, xCtx) = tileExtractor(middle.dataType)(row(input2))
    val (yTile, yCtx) = tileExtractor(right.dataType)(row(input3))

    if (xCtx.isEmpty && yCtx.isDefined)
    logger.warn(
      s"Middle parameter '${middle}' provided an extent and CRS, but the right parameter " +
        s"'${right}' didn't have any. Because the middle defines output type, the right-hand context will be lost.")

    if(xCtx.isDefined && yCtx.isDefined && xCtx != yCtx)
      logger.warn(s"Both '${middle}' and '${right}' provided an extent and CRS, but they are different. The former will be used.")

    val result = op(conditionTile, xTile, yTile)
    toInternalRow(result, xCtx)
  }

  def op(condition: Tile, x: Tile, y: Tile): Tile = {
    import spire.syntax.cfor.cfor
    require(condition.dimensions == x.dimensions)
    require(x.dimensions == y.dimensions)

    val returnTile = x.mutable

    def getSet(c: Int, r: Int): Unit = {
      (returnTile.cellType.isFloatingPoint, y.cellType.isFloatingPoint) match {
        case (true, true) ⇒ returnTile.setDouble(c, r, y.getDouble(c, r))
        case (true, false) ⇒ returnTile.setDouble(c, r, y.get(c, r))
        case (false, true) ⇒ returnTile.set(c, r, y.getDouble(c, r).toInt)
        case (false, false) ⇒ returnTile.set(c, r, y.get(c, r))
      }
    }

    cfor(0)(_ < x.rows, _ + 1) { r ⇒
      cfor(0)(_ < x.cols, _ + 1) { c ⇒
        if(!isCellTrue(condition, c, r)) getSet(c, r)
      }
    }

    returnTile
  }

}
object Where {
  def apply(condition: Column, x: Column, y: Column): Column = new Column(Where(condition.expr, x.expr, y.expr))

}
