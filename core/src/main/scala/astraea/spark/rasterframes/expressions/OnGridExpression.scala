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
import astraea.spark.rasterframes.ref.{RasterRef, RasterSource}
import geotrellis.raster.{Grid, Tile}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.UnaryExpression
import org.apache.spark.sql.rf._
import org.apache.spark.sql.types.DataType

/**
 * Implements boilerplate for subtype expressions processing TileUDT, RasterSourceUDT, and RasterRefs
 * as Grid types.
 *
 * @since 11/4/18
 */
trait OnGridExpression extends UnaryExpression {
  private val toGrid: PartialFunction[DataType, InternalRow ⇒ Grid] = {
    case _: TileUDT ⇒
      (row: InternalRow) ⇒ row.to[Tile]
    case _: RasterSourceUDT ⇒
      (row: InternalRow) ⇒ row.to[RasterSource]
    case t if t.conformsTo(classOf[RasterRef].schema) ⇒
      (row: InternalRow) ⇒ row.to[RasterRef]
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!toGrid.isDefinedAt(child.dataType)) {
      TypeCheckFailure(s"Input type '${child.dataType}' does not conform to `Grid`.")
    }
    else TypeCheckSuccess
  }

  final override protected def nullSafeEval(input: Any): Any = {
    input match {
      case row: InternalRow ⇒
        val g = toGrid(child.dataType)(row)
        eval(g)
      case o ⇒ throw new IllegalArgumentException(s"Unsupported input type: $o")
    }
  }

  /** Implemented by subtypes to process incoming ProjectedRasterLike entity. */
  def eval(grid: Grid): Any

}
