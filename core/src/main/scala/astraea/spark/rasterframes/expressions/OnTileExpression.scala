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
import astraea.spark.rasterframes.encoders.CatalystSerializer
import astraea.spark.rasterframes.encoders.CatalystSerializer._
import astraea.spark.rasterframes.model.TileContext
import astraea.spark.rasterframes.tiles.ProjectedRasterTile
import geotrellis.raster.Tile
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.UnaryExpression
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.rf._

/** Boilerplate for expressions operating on a single Tile-like . */
trait OnTileExpression extends UnaryExpression {
  private val extractor: PartialFunction[DataType, InternalRow => (Tile, Option[TileContext])] = {
    case _: TileUDT =>
      (row: InternalRow) => (row.to[Tile](TileUDT.tileSerializer), None)
    case t if t.conformsTo(CatalystSerializer[ProjectedRasterTile].schema) =>
      (row: InternalRow) => {
        val prt = row.to[ProjectedRasterTile]
        (prt, Some(TileContext(prt)))
      }
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!extractor.isDefinedAt(child.dataType)) {
      TypeCheckFailure(s"Input type '${child.dataType}' does not conform to a raster type.")
    }
    else TypeCheckSuccess
  }

  override protected def nullSafeEval(input: Any): InternalRow = {
    val (tile, ctx) = extractor(child.dataType)(row(input))
    eval(tile, ctx)
  }

  protected def eval(tile: Tile, ctx: Option[TileContext]): InternalRow

}
