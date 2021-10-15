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

package org.locationtech.rasterframes.expressions.generators

import com.typesafe.scalalogging.Logger
import geotrellis.raster.Dimensions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.rasterframes
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.expressions.RasterResult
import org.locationtech.rasterframes.expressions.generators.RasterSourceToRasterRefs.bandNames
import org.locationtech.rasterframes.tiles.ProjectedRasterTile
import org.locationtech.rasterframes.util._
import org.slf4j.LoggerFactory

import scala.util.Try
import scala.util.control.NonFatal

/**
 * Accepts RasterRef and generates one or more RasterRef instances representing the
 * native internal sub-tiling, if any (and requested).
 *
 * @since 9/6/18
 */
case class RasterSourceToTiles(children: Seq[Expression], bandIndexes: Seq[Int], subtileDims: Option[Dimensions[Int]] = None, bufferSize: Short = 0)
  extends Expression with RasterResult with Generator with CodegenFallback with ExpectsInputTypes  {

  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  def inputTypes: Seq[DataType] = Seq.fill(children.size)(rasterSourceUDT)
  override def nodeName: String = "rf_raster_source_to_tiles"

  def elementSchema: StructType = StructType(for {
    child <- children
    basename = child.name
    name <- bandNames(basename, bandIndexes)
  } yield StructField(name, ProjectedRasterTile.projectedRasterTileEncoder.schema, true))

  def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    try {
      val tiles = children.map { child =>
        val src = rasterSourceUDT.deserialize(child.eval(input))
        val maxBands = src.bandCount
        val allowedBands = bandIndexes.filter(_ < maxBands)
        src.readAll(subtileDims.getOrElse(rasterframes.NOMINAL_TILE_DIMS), allowedBands)
          .map(r => bandIndexes.map {
            case i if i < maxBands => ProjectedRasterTile(r.tile.band(i), r.extent, src.crs)
            case _ => null
          })
      }
      tiles
        .transpose
        .map { ts =>
          InternalRow(ts.flatMap(_.map { prt => if (prt != null) toInternalRow(prt) else null }): _*)
        }
    }
    catch {
      case NonFatal(ex) =>
        val payload = Try(children.map(c => rasterSourceUDT.deserialize(c.eval(input)))).toOption.toSeq.flatten
        logger.error("Error fetching data for one of: " + payload.mkString(", "), ex)
        Traversable.empty
    }
  }
}

object RasterSourceToTiles {
  def apply(rrs: Column*): TypedColumn[Any, ProjectedRasterTile] = apply(None, Seq(0), rrs: _*)
  def apply(subtileDims: Option[Dimensions[Int]], bandIndexes: Seq[Int], rrs: Column*): TypedColumn[Any, ProjectedRasterTile] =
    new Column(new RasterSourceToTiles(rrs.map(_.expr), bandIndexes, subtileDims, 0.toShort)).as[ProjectedRasterTile]
  def apply(subtileDims: Option[Dimensions[Int]], bandIndexes: Seq[Int], bufferSize: Short, rrs: Column*): TypedColumn[Any, ProjectedRasterTile] =
    new Column(new RasterSourceToTiles(rrs.map(_.expr), bandIndexes, subtileDims, bufferSize)).as[ProjectedRasterTile]
}


