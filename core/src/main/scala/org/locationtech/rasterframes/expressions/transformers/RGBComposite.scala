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

import geotrellis.raster.ArrayMultibandTile
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, TernaryExpression}
import org.apache.spark.sql.rf.TileUDT
import org.apache.spark.sql.types.DataType
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.expressions.DynamicExtractors.tileExtractor
import org.locationtech.rasterframes.expressions.row

/**
  * Expression to combine the given tile columns into an 32-bit RGB composite.
  * Tiles in each row will first be and-ed with 0xFF, bit shifted, and or-ed into a single 32-bit word.
  * @param red tile column to represent red channel
  * @param green tile column to represent green channel
  * @param blue tile column to represent blue channel
  */
@ExpressionDescription(
  usage = "_FUNC_(red, green, blue) - Combines the given tile columns into an 32-bit RGB composite.",
  arguments = """
  Arguments:
    * red - tile column representing the red channel
    * green - tile column representing the green channel
    * blue - tile column representing the blue channel"""
)
case class RGBComposite(red: Expression, green: Expression, blue: Expression) extends TernaryExpression
  with CodegenFallback {

  override def nodeName: String = "rf_rgb_composite"

  override def dataType: DataType = if(
    tileExtractor.isDefinedAt(red.dataType) ||
      tileExtractor.isDefinedAt(green.dataType) ||
      tileExtractor.isDefinedAt(blue.dataType)
  ) red.dataType
  else TileType

  override def children: Seq[Expression] = Seq(red, green, blue)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!tileExtractor.isDefinedAt(red.dataType)) {
      TypeCheckFailure(s"Red channel input type '${red.dataType}' does not conform to a raster type.")
    }
    else if (!tileExtractor.isDefinedAt(green.dataType)) {
      TypeCheckFailure(s"Green channel input type '${green.dataType}' does not conform to a raster type.")
    }
    else if (!tileExtractor.isDefinedAt(blue.dataType)) {
      TypeCheckFailure(s"Blue channel input type '${blue.dataType}' does not conform to a raster type.")
    }
    else TypeCheckSuccess
  }

  override protected def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
    val (r, rc) = tileExtractor(red.dataType)(row(input1))
    val (g, gc) = tileExtractor(green.dataType)(row(input2))
    val (b, bc) = tileExtractor(blue.dataType)(row(input3))

    // Pick the first available TileContext, if any, and reassociate with the result
    val ctx = Seq(rc, gc, bc).flatten.headOption
    val composite = ArrayMultibandTile(
      r.rescale(0, 255), g.rescale(0, 255), b.rescale(0, 255)
    ).color()
    ctx match {
      case Some(c) => c.toProjectRasterTile(composite).toInternalRow
      case None =>
        implicit val tileSer = TileUDT.tileSerializer
        composite.toInternalRow
    }
  }
}

object RGBComposite {
  def apply(red: Column, green: Column, blue: Column): Column =
    new Column(RGBComposite(red.expr, green.expr, blue.expr))
}
