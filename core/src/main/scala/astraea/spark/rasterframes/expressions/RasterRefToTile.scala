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
import astraea.spark.rasterframes.ref.RasterRef
import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.Tile
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, UnaryExpression}
import org.apache.spark.sql.rf._
import org.apache.spark.sql.types.DataType

/**
 * Realizes a RasterRef into a Tile.
 *
 * @since 11/2/18
 */
case class RasterRefToTile(child: Expression) extends UnaryExpression
  with CodegenFallback with ExpectsInputTypes with LazyLogging {

  override def inputTypes = Seq(classOf[RasterRef].schema)

  override def dataType: DataType = new TileUDT

  override protected def nullSafeEval(input: Any): Any = {
    val ref = row(input).to[RasterRef]
    (ref.tile: Tile).toRow
  }
}

object RasterRefToTile {
  def apply(rr: Column): Column =
    RasterRefToTile(rr.expr).asColumn
}
