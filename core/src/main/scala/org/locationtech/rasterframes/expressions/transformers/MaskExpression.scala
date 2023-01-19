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
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.DataType
import org.locationtech.rasterframes.expressions.DynamicExtractors.tileExtractor

import spire.syntax.cfor._

trait MaskExpression { self: Expression =>

  def targetTile: Expression
  def maskTile: Expression

  def dataType: DataType = targetTile.dataType

  protected lazy val targetTileExtractor = tileExtractor(targetTile.dataType)
  protected lazy val maskTileExtractor = tileExtractor(maskTile.dataType)

  def checkTileDataTypes(): TypeCheckResult = {
    if (!tileExtractor.isDefinedAt(targetTile.dataType)) {
      TypeCheckFailure(s"Input type '${targetTile.dataType}' does not conform to a raster type.")
    } else if (!tileExtractor.isDefinedAt(maskTile.dataType)) {
      TypeCheckFailure(s"Input type '${maskTile.dataType}' does not conform to a raster type.")
    } else TypeCheckSuccess
  }

  def maskEval(targetTile: Tile, maskTile: Tile, maskInt: (Int, Int) => Int, maskDouble: (Double, Int) => Double): Tile = {
    val result = targetTile.mutable

    if (targetTile.cellType.isFloatingPoint) {
      cfor(0)(_ < targetTile.rows, _ + 1) { row =>
        cfor(0)(_ < targetTile.cols, _ + 1) { col =>
          val v = targetTile.getDouble(col, row)
          val m = maskTile.get(col, row)
          result.setDouble(col, row, maskDouble(v, m))
        }
      }
    } else {
      cfor(0)(_ < targetTile.rows, _ + 1) { row =>
        cfor(0)(_ < targetTile.cols, _ + 1) { col =>
          val v = targetTile.get(col, row)
          val m = maskTile.get(col, row)
          result.set(col, row, maskInt(v, m))
        }
      }
    }

    result
  }
}
