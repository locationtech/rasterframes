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

import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.util._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.rf._
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.util.control.NonFatal
import org.locationtech.rasterframes.TileType

/**
 * Accepts RasterRef and generates one or more RasterRef instances representing the
 * native internal sub-tiling, if any (and requested).
 *
 * @since 9/6/18
 */
@deprecated("Use RasterSourceToRasterRefs and RasterRef to Tile instread", "4/28/19")
case class RasterSourceToTiles(children: Seq[Expression], applyTiling: Boolean) extends Expression
  with Generator with CodegenFallback with ExpectsInputTypes with LazyLogging {

  private val RasterSourceType = new RasterSourceUDT()
  override def inputTypes: Seq[DataType] = Seq.fill(children.size)(RasterSourceType)
  override def nodeName: String = "raster_source_to_tile"

  override def elementSchema: StructType = StructType(
    children.map(e ⇒ StructField(e.name, TileType, true))
  )

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    implicit val ser = TileUDT.tileSerializer

    try {
      val refs = children.map { child ⇒
        val src = RasterSourceType.deserialize(child.eval(input))
        val tiles = if (applyTiling) src.readAll() else {
          Seq(src.read(src.extent))
        }
        tiles.map(_.mapTile(_.band(0)))
      }
      refs.transpose.map(ts ⇒ InternalRow(ts.map(r ⇒ r.tile.toInternalRow): _*))
    }
    catch {
      case NonFatal(ex) ⇒
        logger.error("Error fetching data for " + sql, ex)
        Traversable.empty
    }
  }
}


object RasterSourceToTiles {
  @deprecated("Use RasterSourceToRasterRefs and RasterRef to Tile instread", "4/28/19")
  def apply(rrs: Column*): Column = apply(true, rrs: _*)
  @deprecated("Use RasterSourceToRasterRefs and RasterRef to Tile instread", "4/28/19")
  def apply(applyTiling: Boolean, rrs: Column*): Column =
    new Column(new RasterSourceToTiles(rrs.map(_.expr), applyTiling))
}