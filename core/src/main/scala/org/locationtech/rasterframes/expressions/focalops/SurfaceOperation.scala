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
import geotrellis.raster.Tile
import org.apache.spark.sql.rf.TileUDT
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.model.TileContext
import org.locationtech.rasterframes.expressions.{NullToValue, UnaryRasterFunction}

/** Operation on a tile returning a tile. */
trait SurfaceOperation extends UnaryRasterFunction with NullToValue with CodegenFallback  {
  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  override def dataType: DataType = child.dataType
  override def na: Any = null

  override protected def eval(tile: Tile, ctx: Option[TileContext]): Any = {
    implicit val tileSer = TileUDT.tileSerializer

    ctx match {
      case Some(ctx) => ctx.toProjectRasterTile(op(tile, ctx)).toInternalRow
      case None => new NotImplementedError("Surface operation requires ProjectedRasterTile")
    }
  }

  protected def op(t: Tile, ctx: TileContext): Tile
}
