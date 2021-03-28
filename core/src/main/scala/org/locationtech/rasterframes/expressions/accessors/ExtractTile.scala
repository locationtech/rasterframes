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

package org.locationtech.rasterframes.expressions.accessors

import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.expressions.UnaryRasterFunction
import org.locationtech.rasterframes.tiles.ProjectedRasterTile.ConcreteProjectedRasterTile
import geotrellis.raster.Tile
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.rf.TileUDT
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.rasterframes.model.TileContext
import org.locationtech.rasterframes.tiles.InternalRowTile
import org.locationtech.rasterframes._

/** Expression to extract at tile from several types that contain tiles.*/
case class ExtractTile(child: Expression) extends UnaryRasterFunction with CodegenFallback {
  override def dataType: DataType = TileType

  override def nodeName: String = "rf_extract_tile"
  implicit val tileSer = TileUDT.tileSerializer
  override protected def eval(tile: Tile, ctx: Option[TileContext]): Any = tile match {
    case irt: InternalRowTile => irt.mem
    case tile: ConcreteProjectedRasterTile => tile.t.toInternalRow
    case tile: Tile => tile.toInternalRow
  }
}

object ExtractTile {
  import org.locationtech.rasterframes.encoders.StandardEncoders.singlebandTileEncoder
  def apply(input: Column): TypedColumn[Any, Tile] =
    new Column(new ExtractTile(input.expr)).as[Tile]
}
