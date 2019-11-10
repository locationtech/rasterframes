/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2019 Astraea, Inc.
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

package org.locationtech.rasterframes.expressions.transformers

import com.typesafe.scalalogging.Logger
import geotrellis.raster
import geotrellis.raster.Tile
import geotrellis.raster.mapalgebra.local.{Defined, InverseMask => gtInverseMask, Mask => gtMask}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, Expression, ExpressionDescription, Literal, TernaryExpression}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.rf.TileUDT
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.expressions.DynamicExtractors._
import org.locationtech.rasterframes.expressions.localops.IsIn
import org.locationtech.rasterframes.expressions.row
import org.slf4j.LoggerFactory

abstract class Mask(val left: Expression, val middle: Expression, val right: Expression, inverse: Boolean)
  extends TernaryExpression with CodegenFallback with Serializable {
  def targetExp = left
  def maskExp = middle
  def maskValueExp = right

  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  override def children: Seq[Expression] = Seq(left, middle, right)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!tileExtractor.isDefinedAt(targetExp.dataType)) {
      TypeCheckFailure(s"Input type '${targetExp.dataType}' does not conform to a raster type.")
    } else if (!tileExtractor.isDefinedAt(maskExp.dataType)) {
      TypeCheckFailure(s"Input type '${maskExp.dataType}' does not conform to a raster type.")
    } else if (!intArgExtractor.isDefinedAt(maskValueExp.dataType)) {
      TypeCheckFailure(s"Input type '${maskValueExp.dataType}' isn't an integral type.")
    } else TypeCheckSuccess
  }
  override def dataType: DataType = left.dataType

  override def makeCopy(newArgs: Array[AnyRef]): Expression = super.makeCopy(newArgs)

  override protected def nullSafeEval(targetInput: Any, maskInput: Any, maskValueInput: Any): Any = {
    implicit val tileSer = TileUDT.tileSerializer
    val (targetTile, targetCtx) = tileExtractor(targetExp.dataType)(row(targetInput))
    val (maskTile, maskCtx) = tileExtractor(maskExp.dataType)(row(maskInput))

    if (targetCtx.isEmpty && maskCtx.isDefined)
      logger.warn(
          s"Right-hand parameter '${middle}' provided an extent and CRS, but the left-hand parameter " +
            s"'${left}' didn't have any. Because the left-hand side defines output type, the right-hand context will be lost.")

    if (targetCtx.isDefined && maskCtx.isDefined && targetCtx != maskCtx)
      logger.warn(s"Both '${left}' and '${middle}' provided an extent and CRS, but they are different. Left-hand side will be used.")

    val maskValue = intArgExtractor(maskValueExp.dataType)(maskValueInput)

    val masking = if (maskValue.value == 0) Defined(maskTile)
    else maskTile

    val result = if (inverse)
      gtInverseMask(targetTile, masking, maskValue.value, raster.NODATA)
    else
      gtMask(targetTile, masking, maskValue.value, raster.NODATA)

    targetCtx match {
      case Some(ctx) => ctx.toProjectRasterTile(result).toInternalRow
      case None      => result.toInternalRow
    }
  }
}
object Mask {
  import org.locationtech.rasterframes.encoders.StandardEncoders.singlebandTileEncoder

  @ExpressionDescription(
    usage = "_FUNC_(target, mask) - Generate a tile with the values from the data tile, but where cells in the masking tile contain NODATA, replace the data value with NODATA.",
    arguments = """
  Arguments:
    * target - tile to mask
    * mask - masking definition""",
    examples = """
  Examples:
    > SELECT _FUNC_(target, mask);
       ..."""
  )
  case class MaskByDefined(target: Expression, mask: Expression)
    extends Mask(target, mask, Literal(0), false) {
    override def nodeName: String = "rf_mask"
  }
  object MaskByDefined {
    def apply(targetTile: Column, maskTile: Column): TypedColumn[Any, Tile] =
      new Column(MaskByDefined(targetTile.expr, maskTile.expr)).as[Tile]
  }

  @ExpressionDescription(
    usage = "_FUNC_(target, mask) - Generate a tile with the values from the data tile, but where cells in the masking tile DO NOT contain NODATA, replace the data value with NODATA",
    arguments = """
  Arguments:
    * target - tile to mask
    * mask - masking definition""",
    examples = """
  Examples:
    > SELECT _FUNC_(target, mask);
       ..."""
  )
  case class InverseMaskByDefined(leftTile: Expression, rightTile: Expression)
    extends Mask(leftTile, rightTile, Literal(0), true) {
    override def nodeName: String = "rf_inverse_mask"
  }
  object InverseMaskByDefined {
    def apply(srcTile: Column, maskingTile: Column): TypedColumn[Any, Tile] =
      new Column(InverseMaskByDefined(srcTile.expr, maskingTile.expr)).as[Tile]
  }

  @ExpressionDescription(
    usage = "_FUNC_(target, mask, maskValue) - Generate a tile with the values from the data tile, but where cells in the masking tile contain the masking value, replace the data value with NODATA.",
    arguments = """
  Arguments:
    * target - tile to mask
    * mask - masking definition""",
    examples = """
  Examples:
    > SELECT _FUNC_(target, mask, maskValue);
       ..."""
  )
  case class MaskByValue(leftTile: Expression, rightTile: Expression, maskValue: Expression)
    extends Mask(leftTile, rightTile, maskValue, false) {
    override def nodeName: String = "rf_mask_by_value"
  }
  object MaskByValue {
    def apply(srcTile: Column, maskingTile: Column, maskValue: Column): TypedColumn[Any, Tile] =
      new Column(MaskByValue(srcTile.expr, maskingTile.expr, maskValue.expr)).as[Tile]
  }

  @ExpressionDescription(
    usage = "_FUNC_(target, mask, maskValue) - Generate a tile with the values from the data tile, but where cells in the masking tile DO NOT contain the masking value, replace the data value with NODATA.",
    arguments = """
  Arguments:
    * target - tile to mask
    * mask - masking definition
    * maskValue - value in the `mask` for which to mark `target` as data cells
    """,
    examples = """
  Examples:
    > SELECT _FUNC_(target, mask, maskValue);
       ..."""
  )
  case class InverseMaskByValue(leftTile: Expression, rightTile: Expression, maskValue: Expression)
    extends Mask(leftTile, rightTile, maskValue, true) {
    override def nodeName: String = "rf_inverse_mask_by_value"
  }
  object InverseMaskByValue {
    def apply(srcTile: Column, maskingTile: Column, maskValue: Column): TypedColumn[Any, Tile] =
      new Column(InverseMaskByValue(srcTile.expr, maskingTile.expr, maskValue.expr)).as[Tile]
  }

  @ExpressionDescription(
    usage = "_FUNC_(data, mask, maskValues) - Generate a tile with the values from `data` tile but where cells in the `mask` tile are in the `maskValues` list, replace the value with NODATA.",
    arguments = """
  Arguments:
    * target - tile to mask
    * mask - masking definition
    * maskValues - sequence of values to consider as masks candidates
        """,
    examples = """
  Examples:
    > SELECT _FUNC_(data, mask, array(1, 2, 3))
      ..."""
  )
  case class MaskByValues(dataTile: Expression, maskTile: Expression)
    extends Mask(dataTile, maskTile, Literal(1), inverse = false) {
    def this(dataTile: Expression, maskTile: Expression, maskValues: Expression) =
      this(dataTile, IsIn(maskTile, maskValues))
    override def nodeName: String = "rf_mask_by_values"
  }
  object MaskByValues {
    def apply(dataTile: Column, maskTile: Column, maskValues: Column): TypedColumn[Any, Tile] =
      new Column(MaskByValues(dataTile.expr, IsIn(maskTile, maskValues).expr)).as[Tile]
  }
}
