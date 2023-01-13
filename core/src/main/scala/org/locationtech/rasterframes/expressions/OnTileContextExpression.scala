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

package org.locationtech.rasterframes.expressions

import org.locationtech.rasterframes.expressions.DynamicExtractors._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.UnaryExpression
import org.locationtech.rasterframes.model.TileContext

/**
 * Implements boilerplate for subtype expressions processing TileUDT (when ProjectedRasterTile), RasterSourceUDT, and
 * RasterSource-shaped rows.
 *
 * @since 11/3/18
 */
trait OnTileContextExpression extends UnaryExpression {
  override def checkInputDataTypes(): TypeCheckResult = {
    if (!projectedRasterLikeExtractor.isDefinedAt(child.dataType)) {
      TypeCheckFailure(s"Input type '${child.dataType}' does not conform to `ProjectedRasterLike`.")
    }
    else TypeCheckSuccess
  }

  final override protected def nullSafeEval(input: Any): Any = {
    input match {
      case row: InternalRow =>
        val prl = projectedRasterLikeExtractor(child.dataType)(row)
        eval(TileContext(prl.extent, prl.crs))
      case o => throw new IllegalArgumentException(s"Unsupported input type: $o")
    }
  }

  /** Implemented by subtypes to process incoming ProjectedRasterLike entity. */
  def eval(ctx: TileContext): Any
}
