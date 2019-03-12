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

import astraea.spark.rasterframes.encoders.CatalystSerializer._
import astraea.spark.rasterframes.expressions.DynamicExtractors._
import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.Tile
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.UnaryExpression
import org.apache.spark.sql.rf.TileUDT
import org.apache.spark.sql.types.DataType

/** Operation combining two tiles or a tile and a scalar into a new tile. */
trait UnaryLocalRasterOp extends UnaryExpression with LazyLogging {

  override def dataType: DataType = child.dataType

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!tileExtractor.isDefinedAt(child.dataType)) {
      TypeCheckFailure(s"Input type '${child.dataType}' does not conform to a raster type.")
    }
    else TypeCheckSuccess
  }

  override protected def nullSafeEval(input: Any): Any = {
    implicit val tileSer = TileUDT.tileSerializer
    val (childTile, childCtx) = tileExtractor(child.dataType)(row(input))

    childCtx match {
      case Some(ctx) => ctx.toProjectRasterTile(op(childTile)).toInternalRow
      case None => op(childTile).toInternalRow
    }
  }

  protected def op(child: Tile): Tile
}

