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
import astraea.spark.rasterframes.encoders.CatalystSerializer
import astraea.spark.rasterframes.encoders.CatalystSerializer._
import astraea.spark.rasterframes.model.TileContext
import astraea.spark.rasterframes.ref.{ProjectedRasterLike, RasterRef, RasterSource}
import astraea.spark.rasterframes.tiles.ProjectedRasterTile
import geotrellis.raster.{CellGrid, Tile}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.rf.{TileUDT, _}
import org.apache.spark.sql.types._

private[expressions]
object DynamicExtractors {
  /** Partial function for pulling a tile and its contesxt from an input row. */
  lazy val tileExtractor: PartialFunction[DataType, InternalRow => (Tile, Option[TileContext])] = {
    case _: TileUDT =>
      (row: InternalRow) =>
        (row.to[Tile](TileUDT.tileSerializer), None)
    case t if t.conformsTo(CatalystSerializer[ProjectedRasterTile].schema) =>
      (row: InternalRow) => {
        val prt = row.to[ProjectedRasterTile]
        (prt, Some(TileContext(prt)))
      }
  }

  /** Partial function for pulling a ProjectedRasterLike an input row. */
  lazy val projectedRasterLikeExtractor: PartialFunction[DataType, InternalRow ⇒ ProjectedRasterLike] = {
    case _: RasterSourceUDT ⇒
      (row: InternalRow) ⇒ row.to[RasterSource](RasterSourceUDT.rasterSourceSerializer)
    case t if t.conformsTo(CatalystSerializer[ProjectedRasterTile].schema) =>
      (row: InternalRow) => row.to[ProjectedRasterTile]
    case t if t.conformsTo(CatalystSerializer[RasterRef].schema) =>
      (row: InternalRow) ⇒ row.to[RasterRef]
  }

  /** Partial function for pulling a CellGrid from an input row. */
  lazy val gridExtractor: PartialFunction[DataType, InternalRow ⇒ CellGrid] = {
    case _: TileUDT ⇒
      (row: InternalRow) ⇒ row.to[Tile](TileUDT.tileSerializer)
    case _: RasterSourceUDT ⇒
      (row: InternalRow) ⇒ row.to[RasterSource](RasterSourceUDT.rasterSourceSerializer)
    case t if t.conformsTo(CatalystSerializer[RasterRef].schema) ⇒
      (row: InternalRow) ⇒ row.to[RasterRef]
  }

  sealed trait TileOrNumberArg
  case class TileArg(tile: Tile, ctx: Option[TileContext]) extends TileOrNumberArg
  case class DoubleArg(d: Double) extends  TileOrNumberArg
  case class IntegerArg(d: Int) extends  TileOrNumberArg

  lazy val tileOrNumberExtractor: PartialFunction[DataType, Any => TileOrNumberArg] = {
    case t if tileExtractor.isDefinedAt(t) =>
      (in: Any) => {
        val (tile, ctx) = tileExtractor(t)(in.asInstanceOf[InternalRow])
        TileArg(tile, ctx)
      }
    case _: DoubleType | _: FloatType | _: DecimalType =>
      (in: Any) => in match {
        case d: Double => DoubleArg(d)
        case f: Float => DoubleArg(f.toDouble)
        case d: Decimal => DoubleArg(d.toDouble)
      }
    case _: IntegerType | _: ByteType | _: ShortType =>
      (in: Any) => in match {
        case i: Int => IntegerArg(i)
        case b: Byte => IntegerArg(b)
        case s: Short => IntegerArg(s.toInt)
        case c: Char => IntegerArg(c.toInt)
      }
  }
}
