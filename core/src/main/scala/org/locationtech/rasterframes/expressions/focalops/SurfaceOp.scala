/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2021 Azavea, Inc.
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

package org.locationtech.rasterframes.expressions.focalops

import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.types.DataType
import org.locationtech.rasterframes.expressions.row
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.locationtech.rasterframes.ref.RasterRef

import org.locationtech.rasterframes.encoders.syntax._
import org.locationtech.rasterframes.expressions.DynamicExtractors._
import geotrellis.raster.Tile
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.UnaryExpression
import org.locationtech.rasterframes.model.TileContext
import org.locationtech.rasterframes.expressions.NullToValue
import org.locationtech.rasterframes.tiles.ProjectedRasterTile

/** Operation on a tile returning a tile. */
trait SurfaceOp extends UnaryExpression with NullToValue with CodegenFallback  {
  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  def dataType: DataType = child.dataType
  def na: Any = null

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!tileExtractor.isDefinedAt(child.dataType)) {
      TypeCheckFailure(s"Input type '${child.dataType}' does not conform to a raster type.")
    } else TypeCheckSuccess
  }

  override protected def nullSafeEval(input: Any): Any = {
    val (tile, ctx) = tileExtractor(child.dataType)(row(input))

    val literral = tile match {
      // if it is RasterRef, we want the BufferTile
      case ref: RasterRef => ref.realizedTile
      // if it is a ProjectedRasterTile, can we flatten it?
      case prt: ProjectedRasterTile => prt.tile match {
        // if it is RasterRef, we can get what's inside
        case rr: RasterRef => rr.realizedTile
        // otherwise it is some tile
        case _             => prt.tile
      }
    }
    eval(literral, ctx)
  }

  protected def eval(tile: Tile, ctx: Option[TileContext]): Any = {
    ctx match {
      case Some(ctx) =>
        val ret = op(tile, ctx)
        ctx.toProjectRasterTile(ret).toInternalRow

      case None => new NotImplementedError("Surface operation requires ProjectedRasterTile")
    }
  }

  protected def op(t: Tile, ctx: TileContext): Tile
}
