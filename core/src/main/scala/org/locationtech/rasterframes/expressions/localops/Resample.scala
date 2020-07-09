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
import geotrellis.raster.resample._
import geotrellis.raster.resample.{Max ⇒ RMax, Min ⇒ RMin}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, Literal, TernaryExpression}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.rf.TileUDT
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.expressions.{fpTile, row}
import org.locationtech.rasterframes.expressions.DynamicExtractors._


abstract class ResampleBase(left: Expression, right: Expression, method: Expression)
  extends TernaryExpression
  with CodegenFallback with Serializable {

  override val nodeName: String = "rf_resample"
  override def dataType: DataType = left.dataType
  override def children: Seq[Expression] = Seq(left, right, method)

  def targetFloatIfNeeded(t: Tile, method: ResampleMethod): Tile =
     method match {
      case NearestNeighbor | Mode | RMax | RMin | Sum  ⇒ t
      case _ ⇒ fpTile(t)
     }

  def stringToMethod(methodName: String): ResampleMethod =
    methodName.toLowerCase().trim().replaceAll("_", "") match {
      case "nearestneighbor" | "nearest" ⇒ NearestNeighbor
      case "bilinear" ⇒ Bilinear
      case "cubicconvolution" ⇒ CubicConvolution
      case "cubicspline" ⇒ CubicSpline
      case "lanczos" | "lanzos" ⇒ Lanczos
      // aggregates
      case "average" ⇒ Average
      case "mode" ⇒ Mode
      case "median" ⇒ Median
      case "max" ⇒ RMax
      case "min" ⇒ RMin
      case "sum" ⇒ Sum
    }

   // These methods define the core algorithms to be used.
  def op(left: Tile, right: Tile, method: String): Tile = {
    val m = stringToMethod(method)
    targetFloatIfNeeded(left, m)
      .resample(right.cols, right.rows, m)
  }

  def op(left: Tile, right: Double, method: String): Tile = {
    val m = stringToMethod(method)
    targetFloatIfNeeded(left, m)
      .resample((left.cols * right).toInt, (left.rows * right).toInt, m)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    // copypasta from BinaryLocalRasterOp
    if (!tileExtractor.isDefinedAt(left.dataType)) {
      TypeCheckFailure(s"Input type '${left.dataType}' does not conform to a raster type.")
    }
    else if (!tileOrNumberExtractor.isDefinedAt(right.dataType)) {
      TypeCheckFailure(s"Input type '${right.dataType}' does not conform to a compatible type.")
    } else method.dataType match {
      case StringType ⇒ TypeCheckSuccess
      case _ ⇒ TypeCheckFailure(s"Cannot interpret value of type `${method.dataType.simpleString}` for resampling method; please provide a String method name.")
    }
  }

  override def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
    // more copypasta from BinaryLocalRasterOp
    implicit val tileSer = TileUDT.tileSerializer

    val (leftTile, leftCtx) = tileExtractor(left.dataType)(row(input1))
    val methodString = input3.asInstanceOf[UTF8String].toString

    val result: Tile = tileOrNumberExtractor(right.dataType)(input2) match {
      // in this case we expect the left and right contexts to vary. no warnings raised.
      case TileArg(rightTile, _) ⇒ op(leftTile, rightTile, methodString)
      case DoubleArg(d) ⇒ op(leftTile, d, methodString)
      case IntegerArg(i) ⇒ op(leftTile, i.toDouble, methodString)
    }

    // reassemble the leftTile with its context. Note that this operation does not change Extent and CRS
    leftCtx match {
      case Some(ctx) ⇒ ctx.toProjectRasterTile(result).toInternalRow
      case None ⇒ result.toInternalRow
    }
  }

  override def eval(input: InternalRow): Any = {
    if(input == null) null
    else {
      val l = left.eval(input)
      val r = right.eval(input)
      val m = method.eval(input)
      if (m == null) null // no method, return null
      else if (l == null) null  // no l tile, return null
      else if (r == null) l // no target tile or factor, return l without changin it
      else nullSafeEval(l, r, m)
    }
  }

}

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
case class Resample(left: Expression, factor: Expression, method: Expression)
  extends ResampleBase(left, factor, method)

object Resample {
  def apply(left: Column, right: Column, methodName: String): Column =
    new Column(Resample(left.expr, right.expr, lit(methodName).expr))

  def apply(left: Column, right: Column, method: Column): Column =
    new Column(Resample(left.expr, right.expr, method.expr))

  def apply[N: Numeric](left: Column, right: N, method: String): Column = new Column(Resample(left.expr, lit(right).expr, lit(method).expr))

  def apply[N: Numeric](left: Column, right: N, method: Column): Column = new Column(Resample(left.expr, lit(right).expr, method.expr))

}

@ExpressionDescription(
 usage = "_FUNC_(tile, factor) - Resample tile to different size based on scalar factor or tile whose dimension to match. Scalar less than one will downsample tile; greater than one will upsample. Uses nearest-neighbor value.",
 arguments = """
  Arguments:
    * tile - tile
    * rhs  - scalar or tile to match dimension""",
 examples = """
  Examples:
    > SELECT _FUNC_(tile, 2.0);
       ...
    > SELECT _FUNC_(tile1, tile2);
       ...""")
case class ResampleNearest(tile: Expression, target: Expression)
  extends ResampleBase(tile, target, Literal("nearest"))
object ResampleNearest {
  def apply(tile: Column, target: Column): Column =
    new Column(ResampleNearest(tile.expr, target.expr))

  def apply[N: Numeric](tile: Column, value: N): Column =
    new Column(ResampleNearest(tile.expr, lit(value).expr))
}


