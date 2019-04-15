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

package astraea.spark.rasterframes.expressions.transformers

import astraea.spark.rasterframes.encoders.CatalystSerializer._
import astraea.spark.rasterframes.model.TileDimensions
import astraea.spark.rasterframes.ref.{RasterRef, RasterSource}
import astraea.spark.rasterframes.util._
import com.typesafe.scalalogging.LazyLogging
import geotrellis.vector.Extent
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.rf._
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{Column, TypedColumn}

import scala.util.control.NonFatal

/**
 * Accepts RasterRef and generates one or more RasterRef instances representing the
 * native internal sub-tiling, if any (and requested).
 *
 * @since 9/6/18
 */
case class RasterSourceToRasterRefs(children: Seq[Expression], bandIndexes: Seq[Int], subtileDims: Option[TileDimensions] = None) extends Expression
  with Generator with CodegenFallback with ExpectsInputTypes with LazyLogging {

  private val RasterSourceType = new RasterSourceUDT()
  private val rasterRefSchema = schemaOf[RasterRef]

  override def inputTypes: Seq[DataType] = Seq.fill(children.size)(RasterSourceType)
  override def nodeName: String = "raster_source_to_raster_ref"

  override def elementSchema: StructType = StructType(for {
    child <- children
    band <- bandIndexes
    name = child.name + (if (bandIndexes.length > 1) "_b" + band else "")
  } yield StructField(name, rasterRefSchema, true))

  private def band2ref(src: RasterSource, e: Option[Extent])(b: Int): RasterRef =
    if (b < src.bandCount)
      RasterRef(src, b, e)
    else null

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    try {
      val refs = children.map { child ⇒
        val src = RasterSourceType.deserialize(child.eval(input))
        subtileDims.map(dims =>
          src
            .layoutExtents(dims)
            .map(e ⇒ bandIndexes.map(band2ref(src, Some(e))))
        )
        .getOrElse(Seq(bandIndexes.map(band2ref(src, None))))
      }
      refs.transpose.map(ts ⇒ InternalRow(ts.flatMap(_.map(_.toInternalRow)): _*))
    }
    catch {
      case NonFatal(ex) ⇒
        logger.error("Error fetching data for " + input, ex)
        Traversable.empty
    }
  }
}

object RasterSourceToRasterRefs {
  def apply(rrs: Column*): TypedColumn[Any, RasterRef] = apply(None, Seq(0), rrs: _*)
  def apply(subtileDims: Option[TileDimensions], bandIndexes: Seq[Int], rrs: Column*): TypedColumn[Any, RasterRef] =
    new Column(new RasterSourceToRasterRefs(rrs.map(_.expr), bandIndexes, subtileDims)).as[RasterRef]
}
