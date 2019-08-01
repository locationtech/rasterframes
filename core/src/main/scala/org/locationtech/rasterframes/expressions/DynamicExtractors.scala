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

import geotrellis.raster.{CellGrid, Tile}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.rf.{TileUDT, RasterSourceUDT}
import org.apache.spark.sql.types._
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.model.TileContext
import org.locationtech.rasterframes.ref.{ProjectedRasterLike, RasterRef, RasterSource}
import org.locationtech.rasterframes.tiles.ProjectedRasterTile

private[rasterframes]
object DynamicExtractors {
  /** Partial function for pulling a tile and its contesxt from an input row. */
  lazy val tileExtractor: PartialFunction[DataType, InternalRow => (Tile, Option[TileContext])] = {
    case _: TileUDT =>
      (row: InternalRow) =>
        (row.to[Tile](TileUDT.tileSerializer), None)
    case t if t.conformsTo[ProjectedRasterTile] =>
      (row: InternalRow) => {
        val prt = row.to[ProjectedRasterTile]
        (prt, Some(TileContext(prt)))
      }
  }

  lazy val rowTileExtractor: PartialFunction[DataType, Row => (Tile, Option[TileContext])] = {
    case _: TileUDT =>
      (row: Row) =>  (row.to[Tile](TileUDT.tileSerializer), None)
    case t if t.conformsTo[ProjectedRasterTile] =>
      (row: Row) => {
        val prt = row.to[ProjectedRasterTile]
        (prt, Some(TileContext(prt)))
      }
  }

  /** Partial function for pulling a ProjectedRasterLike an input row. */
  lazy val projectedRasterLikeExtractor: PartialFunction[DataType, InternalRow ⇒ ProjectedRasterLike] = {
    case _: RasterSourceUDT ⇒
      (row: InternalRow) => row.to[RasterSource](RasterSourceUDT.rasterSourceSerializer)
    case t if t.conformsTo[ProjectedRasterTile] =>
      (row: InternalRow) => row.to[ProjectedRasterTile]
    case t if t.conformsTo[RasterRef] =>
      (row: InternalRow) => row.to[RasterRef]
  }

  /** Partial function for pulling a CellGrid from an input row. */
  lazy val gridExtractor: PartialFunction[DataType, InternalRow ⇒ CellGrid] = {
    case _: TileUDT =>
      (row: InternalRow) => row.to[Tile](TileUDT.tileSerializer)
    case _: RasterSourceUDT =>
      (row: InternalRow) => row.to[RasterSource](RasterSourceUDT.rasterSourceSerializer)
    case t if t.conformsTo[RasterRef] ⇒
      (row: InternalRow) => row.to[RasterRef]
    case t if t.conformsTo[ProjectedRasterTile] =>
      (row: InternalRow) => row.to[ProjectedRasterTile]
  }

  sealed trait TileOrNumberArg
  sealed trait NumberArg extends TileOrNumberArg
  case class TileArg(tile: Tile, ctx: Option[TileContext]) extends TileOrNumberArg
  case class DoubleArg(value: Double) extends NumberArg
  case class IntegerArg(value: Int) extends NumberArg

  lazy val tileOrNumberExtractor: PartialFunction[DataType, Any => TileOrNumberArg] =
    tileArgExtractor.orElse(numberArgExtractor)

  lazy val tileArgExtractor: PartialFunction[DataType, Any => TileArg] = {
    case t if tileExtractor.isDefinedAt(t) => {
      case ir: InternalRow =>
        val (tile, ctx) = tileExtractor(t)(ir)
        TileArg(tile, ctx)
    }
  }

  lazy val numberArgExtractor: PartialFunction[DataType, Any => NumberArg] =
    doubleArgExtractor.orElse(intArgExtractor)

  lazy val doubleArgExtractor: PartialFunction[DataType, Any => DoubleArg] = {
    case _: DoubleType | _: FloatType | _: DecimalType => {
      case d: Double  => DoubleArg(d)
      case f: Float   => DoubleArg(f.toDouble)
      case d: Decimal => DoubleArg(d.toDouble)
    }
  }

  lazy val intArgExtractor: PartialFunction[DataType, Any => IntegerArg] = {
    case _: IntegerType | _: ByteType | _: ShortType => {
      case i: Int   => IntegerArg(i)
      case b: Byte  => IntegerArg(b.toInt)
      case s: Short => IntegerArg(s.toInt)
      case c: Char  => IntegerArg(c.toInt)
    }
  }
}
