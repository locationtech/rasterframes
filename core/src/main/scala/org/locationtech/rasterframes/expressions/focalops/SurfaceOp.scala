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

import org.locationtech.rasterframes.expressions.{NullToValue, RasterResult, UnaryRasterFunction, row}
import org.locationtech.rasterframes.encoders.syntax._
import org.locationtech.rasterframes.expressions.DynamicExtractors._
import org.locationtech.rasterframes.model.TileContext
import geotrellis.raster.Tile
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.Logger

/** Operation on a tile returning a tile. */
trait SurfaceOp extends UnaryRasterFunction with RasterResult with NullToValue with CodegenFallback {
  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  def na: Any = null
  def dataType: DataType = child.dataType

  override protected def nullSafeEval(input: Any): Any = {
    val (tile, ctx) = tileExtractor(child.dataType)(row(input))
    eval(extractBufferTile(tile), ctx)
  }

  override protected def eval(tile: Tile, ctx: Option[TileContext]): Any = ctx match {
    case Some(ctx) => ctx.toProjectRasterTile(op(tile, ctx)).toInternalRow
    case None      => new NotImplementedError("Surface operation requires ProjectedRasterTile")
  }

  protected def op(t: Tile, ctx: TileContext): Tile
}
