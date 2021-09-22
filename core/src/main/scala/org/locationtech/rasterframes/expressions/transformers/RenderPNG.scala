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

import geotrellis.raster.Tile
import geotrellis.raster.render.ColorRamp
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.types.{BinaryType, DataType}
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.rasterframes.encoders.SparkBasicEncoders._
import org.locationtech.rasterframes.expressions.UnaryRasterFunction
import org.locationtech.rasterframes.model.TileContext

/**
  * Converts a tile into a PNG encoded byte array.
  * @param child tile column
  * @param ramp color ramp to use for non-composite tiles.
  */
abstract class RenderPNG(child: Expression, ramp: Option[ColorRamp]) extends UnaryRasterFunction with CodegenFallback with Serializable {
  def dataType: DataType = BinaryType
  protected def eval(tile: Tile, ctx: Option[TileContext]): Any = {
    val png = ramp.map(tile.renderPng).getOrElse(tile.renderPng())
    png.bytes
  }
}

object RenderPNG {
  @ExpressionDescription(
    usage = "_FUNC_(tile) - Encode the given tile into a RGB composite PNG. Assumes the red, green, and " +
      "blue channels are encoded as 8-bit channels within the 32-bit word.",
    arguments = """
  Arguments:
    * tile - tile to render"""
  )
  case class RenderCompositePNG(child: Expression) extends RenderPNG(child,  None) {
    override def nodeName: String = "rf_render_png"
  }

  object RenderCompositePNG {
    def apply(red: Column, green: Column, blue: Column): TypedColumn[Any, Array[Byte]] =
      new Column(RenderCompositePNG(RGBComposite(red.expr, green.expr, blue.expr))).as[Array[Byte]]
  }

  @ExpressionDescription(
    usage = "_FUNC_(tile) - Encode the given tile as a PNG using a color ramp with assignemnts from quantile computation",
    arguments = """
  Arguments:
    * tile - tile to render"""
  )
  case class RenderColorRampPNG(child: Expression, colors: ColorRamp) extends RenderPNG(child,  Some(colors)) {
    override def nodeName: String = "rf_render_png"
  }

  object RenderColorRampPNG {
    def apply(tile: Column, colors: ColorRamp): TypedColumn[Any, Array[Byte]] =
      new Column(RenderColorRampPNG(tile.expr, colors)).as[Array[Byte]]
  }
}
