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

package astraea.spark.rasterframes.expressions
import astraea.spark.rasterframes.ref.{ProjectedRasterLike, RasterRef}
import geotrellis.raster.Tile
import org.apache.spark.sql.{Column, TypedColumn}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.rf.TileUDT
import org.apache.spark.sql.types.DataType

/** Expression to extract at tile from several types that contain tiles.*/
case class ExtractTile(child: Expression) extends OnProjectedRasterExpression with CodegenFallback {
  override def dataType: DataType = new TileUDT()

  override def nodeName: String = "extract_tile"

  /** Implemented by subtypes to process incoming ProjectedRasterLike entity. */
  override def eval(prl: ProjectedRasterLike): Any = {
    val result = prl match {
      case rr: RasterRef => rr.tile
      case t: Tile       => t
      case _             => null
    }

    TileUDT.tileSerializer.toInternalRow(result)
  }
}
object ExtractTile {
  import astraea.spark.rasterframes.encoders.StandardEncoders.singlebandTileEncoder
  def apply(input: Column): TypedColumn[Any, Tile] =
    new Column(new ExtractTile(input.expr)).as[Tile]
}
