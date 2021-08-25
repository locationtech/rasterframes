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

package org.locationtech.rasterframes.expressions

import com.typesafe.scalalogging.Logger
import geotrellis.raster.Tile
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.BinaryExpression
import org.apache.spark.sql.types.DataType
import org.locationtech.rasterframes.expressions.DynamicExtractors.tileExtractor
import org.slf4j.LoggerFactory

/** Operation combining two tiles into a new tile. */
trait BinaryRasterOp extends BinaryExpression with RasterResult {
  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  override def dataType: DataType = left.dataType

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!tileExtractor.isDefinedAt(left.dataType)) {
      TypeCheckFailure(s"Input type '${left.dataType}' does not conform to a raster type.")
    }
    else if (!tileExtractor.isDefinedAt(right.dataType)) {
      TypeCheckFailure(s"Input type '${right.dataType}' does not conform to a raster type.")
    }
    else TypeCheckSuccess
  }

  protected def op(left: Tile, right: Tile): Tile

  override protected def nullSafeEval(input1: Any, input2: Any): Any = {
    val (leftTile, leftCtx) = tileExtractor(left.dataType)(row(input1))
    val (rightTile, rightCtx) = tileExtractor(right.dataType)(row(input2))

    if (leftCtx.isEmpty && rightCtx.isDefined)
      logger.warn(
          s"Right-hand parameter '${right}' provided an extent and CRS, but the left-hand parameter " +
            s"'${left}' didn't have any. Because the left-hand side defines output type, the right-hand context will be lost.")

    if(leftCtx.isDefined && rightCtx.isDefined && leftCtx != rightCtx)
      logger.warn(s"Both '${left}' and '${right}' provided an extent and CRS, but they are different. Left-hand side will be used.")

    val result = op(leftTile, rightTile)

    toInternalRow(result, leftCtx)
  }
}
