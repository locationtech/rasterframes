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

import geotrellis.raster.render.ascii.AsciiArtEncoder
import geotrellis.raster.{Tile, isNoData}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.sql.{Column, TypedColumn}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.rasterframes.encoders.SparkBasicEncoders._
import org.locationtech.rasterframes.expressions.UnaryRasterOp
import org.locationtech.rasterframes.model.TileContext
import spire.syntax.cfor.cfor

abstract class DebugRender(asciiArt: Boolean) extends UnaryRasterOp
  with CodegenFallback with Serializable {
  import org.locationtech.rasterframes.expressions.transformers.DebugRender.TileAsMatrix
  override def dataType: DataType = StringType

  override protected def eval(tile: Tile, ctx: Option[TileContext]): Any = {
    UTF8String.fromString(if (asciiArt)
      s"\n${tile.renderAscii(AsciiArtEncoder.Palette.NARROW)}\n"
    else
      s"\n${tile.renderMatrix(6)}\n"
    )
  }
}

object DebugRender {
  @ExpressionDescription(
    usage = "_FUNC_(tile) - Coverts the contents of the given tile an ASCII art string rendering",
    arguments = """
  Arguments:
    * tile - tile to render"""
  )
  case class RenderAscii(child: Expression) extends DebugRender(true) {
    override def nodeName: String = "rf_render_ascii"
  }
  object RenderAscii {
    def apply(tile: Column): TypedColumn[Any, String] =
      new Column(RenderAscii(tile.expr)).as[String]
  }

  @ExpressionDescription(
    usage = "_FUNC_(tile) - Coverts the contents of the given tile to a 2-d array of numberic values",
    arguments = """
  Arguments:
    * tile - tile to render"""
  )
  case class RenderMatrix(child: Expression) extends DebugRender(false) {
    override def nodeName: String = "rf_render_matrix"
  }
  object RenderMatrix {
    def apply(tile: Column): TypedColumn[Any, String] =
      new Column(RenderMatrix(tile.expr)).as[String]
  }

  implicit class TileAsMatrix(val tile: Tile) extends AnyVal {
    def renderMatrix(significantDigits: Int): String = {
      val ND = s"%${significantDigits+5}s".format(Double.NaN)
      val fmt = s"% ${significantDigits+5}.${significantDigits}g"
      val buf = new StringBuilder("[")
      cfor(0)(_ < tile.rows, _ + 1) { row =>
        if(row > 0) buf.append(' ')
        buf.append('[')
        cfor(0)(_ < tile.cols, _ + 1) { col =>
          val v = tile.getDouble(col, row)
          if (isNoData(v)) buf.append(ND)
          else buf.append(fmt.format(v))

          if (col < tile.cols - 1)
            buf.append(',')
        }
        buf.append(']')
        if (row < tile.rows - 1)
          buf.append(",\n")
      }
      buf.append("]")
      buf.toString()
    }
  }
}
