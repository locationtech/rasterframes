/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea, Inc.
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

import astraea.spark.rasterframes.encoders.CatalystSerializer._
import astraea.spark.rasterframes.ref.{ProjectedRasterLike, RasterRef, RasterSource}
import astraea.spark.rasterframes.tiles.ProjectedRasterTile
import geotrellis.raster.Tile
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.UnaryExpression
import org.apache.spark.sql.rf._
import org.apache.spark.sql.types.DataType

/**
 * Implements boilerplate for subtype expressions processing TileUDT, RasterSourceUDT, and
 * RasterSource-shaped rows.
 *
 * @since 11/3/18
 */
trait OnProjectedRasterExpression extends UnaryExpression {

  private val toPRL: PartialFunction[DataType, InternalRow ⇒ ProjectedRasterLike] = {
    case _: TileUDT ⇒
      (row: InternalRow) ⇒ {
        val tile = row.to[Tile]
        tile match {
          case pr: ProjectedRasterTile ⇒ pr
          case _ ⇒ null
        }
    }
    case _: RasterSourceUDT ⇒
      (row: InternalRow) ⇒ row.to[RasterSource]
    case t if t.conformsTo(classOf[RasterRef].schema) ⇒
      (row: InternalRow) ⇒ row.to[RasterRef]
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!toPRL.isDefinedAt(child.dataType)) {
      TypeCheckFailure(s"Input type '${child.dataType}' does not conform to `ProjectedRasterLike`.")
    }
    else TypeCheckSuccess
  }

  final override protected def nullSafeEval(input: Any): Any = {
    input match {
      case row: InternalRow ⇒
        val prl = toPRL(child.dataType)(row)
        eval(prl)
      case o ⇒ throw new IllegalArgumentException(s"Unsupported input type: $o")
    }
  }

  /** Implemented by subtypes to process incoming ProjectedRasterLike entity. */
  def eval(prl: ProjectedRasterLike): Any


}
