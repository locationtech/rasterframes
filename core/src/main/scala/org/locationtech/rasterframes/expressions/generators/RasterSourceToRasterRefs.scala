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

import geotrellis.raster.{Dimensions, GridBounds}
import geotrellis.vector.Extent
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.encoders._
import org.locationtech.rasterframes.expressions.generators.RasterSourceToRasterRefs.bandNames
import org.locationtech.rasterframes.ref.{RFRasterSource, RasterRef}
import org.locationtech.rasterframes.util._
import org.locationtech.rasterframes.ref.Subgrid
import org.locationtech.rasterframes.tiles.ProjectedRasterTile

import scala.util.Try
import scala.util.control.NonFatal

/**
 * Accepts RasterSource and generates one or more RasterRef instances representing
 *
 * @since 9/6/18
 */
case class RasterSourceToRasterRefs(children: Seq[Expression], bandIndexes: Seq[Int], subtileDims: Option[Dimensions[Int]] = None) extends Expression
  with Generator with CodegenFallback with ExpectsInputTypes {

  def inputTypes: Seq[DataType] = Seq.fill(children.size)(rasterSourceUDT)
  override def nodeName: String = "rf_raster_source_to_raster_ref"

  private lazy val enc = ProjectedRasterTile.projectedRasterTileEncoder
  private lazy val prtSerializer = SerializersCache.serializer[ProjectedRasterTile]

  def elementSchema: StructType = StructType(for {
    child <- children
    basename = child.name + "_ref"
    name <- bandNames(basename, bandIndexes)
  } yield StructField(name, enc.schema, true))

  private def band2ref(src: RFRasterSource, grid: Option[GridBounds[Int]], extent: Option[Extent])(b: Int): RasterRef =
    if (b < src.bandCount) RasterRef(src, b, extent, grid.map(Subgrid.apply)) else null


  def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    try {
      val refs = children.map { child =>
        // TODO: we're using the UDT here ... which is what we should do ?
        // what would have serialized it, UDT?
        val src = rasterSourceUDT.deserialize(child.eval(input))
        val srcRE = src.rasterExtent
        subtileDims.map(dims => {
          val subGB = src.layoutBounds(dims)
          val subs = subGB.map(gb => (gb, srcRE.extentFor(gb, clamp = true)))

          subs.map{ case (grid, extent) => bandIndexes.map(band2ref(src, Some(grid), Some(extent))) }
        }).getOrElse(Seq(bandIndexes.map(band2ref(src, None, None))))
      }

      val out = refs.transpose.map(ts =>
        InternalRow(ts.flatMap(_.map{ r =>
          prtSerializer(r: ProjectedRasterTile).copy()
        }): _*))

      out
    }
    catch {
      case NonFatal(ex) =>
        val description = "Error fetching data for one of: " +
          Try(children.map(c => rasterSourceUDT.deserialize(c.eval(input))))
            .toOption.toSeq.flatten.mkString(", ")
        throw new java.lang.IllegalArgumentException(description, ex)
    }
  }
}

object RasterSourceToRasterRefs {
  def apply(rrs: Column*): TypedColumn[Any, ProjectedRasterTile] = apply(None, Seq(0), rrs: _*)
  def apply(subtileDims: Option[Dimensions[Int]], bandIndexes: Seq[Int], rrs: Column*): TypedColumn[Any, ProjectedRasterTile] =
    new Column(new RasterSourceToRasterRefs(rrs.map(_.expr), bandIndexes, subtileDims)).as[ProjectedRasterTile]

  private[rasterframes] def bandNames(basename: String, bandIndexes: Seq[Int]): Seq[String] = bandIndexes match {
    case Seq() => Seq.empty
    case Seq(0) => Seq(basename)
    case s => s.map(n => basename + "_b" + n)
  }
}
