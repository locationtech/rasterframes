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

package org.locationtech.rasterframes.expressions.localops

import geotrellis.raster.Tile
import geotrellis.raster.resample.{Mode, NearestNeighbor, Sum, Max => RMax, Min => RMin, ResampleMethod => GTResampleMethod}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, TernaryExpression}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.rasterframes.expressions.DynamicExtractors._
import org.locationtech.rasterframes.expressions.{RasterResult, fpTile, row}
import org.locationtech.rasterframes.util.ResampleMethod


@ExpressionDescription(
  usage = "_FUNC_(tile, factor, method_name) - Resample tile to different dimension based on scalar `factor` or a tile whose dimension to match. Scalar less than one will downsample tile; greater than one will upsample. Uses resampling method named in the `method_name`." +
    "Methods average, mode, median, max, min, and sum aggregate over cells when downsampling",
  arguments = """
Arguments:
  * tile - tile
  * factor  - scalar or tile to match dimension
  * method_name - one the following options: nearest_neighbor, bilinear, cubic_convolution, cubic_spline, lanczos, average, mode, median, max, min, sum
                  This option can be CamelCase as well
""",
  examples = """
Examples:
  > SELECT _FUNC_(tile, 0.2, median);
     ...
  > SELECT _FUNC_(tile1, tile2, lit("cubic_spline"));
     ..."""
)
case class Resample(tile: Expression, factor: Expression, method: Expression) extends TernaryExpression with RasterResult with CodegenFallback {
  override val nodeName: String = "rf_resample"
  def dataType: DataType = tile.dataType
  def first: Expression = tile
  def second: Expression = factor
  def third: Expression = method

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!tileExtractor.isDefinedAt(tile.dataType)) {
      TypeCheckFailure(s"Input type '${tile.dataType}' does not conform to a raster type.")
    } else if (!tileOrNumberExtractor.isDefinedAt(factor.dataType)) {
      TypeCheckFailure(s"Input type '${factor.dataType}' does not conform to a compatible type.")
    } else
      method.dataType match {
        case StringType => TypeCheckSuccess
        case _ =>
          TypeCheckFailure(
            s"Cannot interpret value of type `${method.dataType.simpleString}` for resampling method; please provide a String method name."
          )
      }
  }
  override def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
    val (leftTile, leftCtx) = tileExtractor(tile.dataType)(row(input1))
    val ton = tileOrNumberExtractor(factor.dataType)(input2)
    val methodString = input3.asInstanceOf[UTF8String].toString
    val resamplingMethod = methodString match {
      case ResampleMethod(mm) => mm
      case _ => throw new IllegalArgumentException("Unrecognized resampling method specified")
    }

    val result: Tile = Resample.op(leftTile, ton, resamplingMethod)
    toInternalRow(result, leftCtx)
  }

  override protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): Expression = copy(newFirst, newSecond, newThird)
}

object Resample {
  def op(tile: Tile, target: TileOrNumberArg, method: GTResampleMethod): Tile = {
    val sourceTile = method match {
      case NearestNeighbor | Mode | RMax | RMin | Sum => tile
      case _ => fpTile(tile)
    }
    target match {
      case TileArg(targetTile, _) =>
        sourceTile.resample(targetTile.cols, targetTile.rows, method)
      case DoubleArg(d) =>
        sourceTile.resample((tile.cols * d).toInt, (tile.rows * d).toInt, method)
      case IntegerArg(i) =>
        sourceTile.resample(tile.cols * i,tile.rows * i, method)
    }
  }

  def apply(tile: Column, factor: Column, methodName: String): Column =
    new Column(Resample(tile.expr, factor.expr, lit(methodName).expr))

  def apply(tile: Column, factor: Column, method: Column): Column =
    new Column(Resample(tile.expr, factor.expr, method.expr))

  def apply[N: Numeric](tile: Column, factor: N, method: String): Column =
    new Column(Resample(tile.expr, lit(factor).expr, lit(method).expr))

  def apply[N: Numeric](tile: Column, factor: N, method: Column): Column =
    new Column(Resample(tile.expr, lit(factor).expr, method.expr))
}






