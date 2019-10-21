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

import geotrellis.raster.GridBounds
import geotrellis.vector.Extent
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.expressions.generators.RasterSourceToRasterRefs.bandNames
import org.locationtech.rasterframes.model.TileDimensions
import org.locationtech.rasterframes.ref.{RasterRef, RasterSource}
import org.locationtech.rasterframes.util._
import org.locationtech.rasterframes.RasterSourceType

import scala.util.Try
import scala.util.control.NonFatal

/**
 * Accepts RasterSource and generates one or more RasterRef instances representing
 *
 * @since 9/6/18
 */
case class RasterSourceToRasterRefs(children: Seq[Expression], bandIndexes: Seq[Int], subtileDims: Option[TileDimensions] = None) extends Expression
  with Generator with CodegenFallback with ExpectsInputTypes {

  override def inputTypes: Seq[DataType] = Seq.fill(children.size)(RasterSourceType)
  override def nodeName: String = "rf_raster_source_to_raster_ref"

  override def elementSchema: StructType = StructType(for {
    child <- children
    basename = child.name + "_ref"
    name <- bandNames(basename, bandIndexes)
  } yield StructField(name, schemaOf[RasterRef], true))

  private def band2ref(src: RasterSource, e: Option[(GridBounds[Int], Extent)])(b: Int): RasterRef =
    if (b < src.bandCount) RasterRef(src, b, e.map(_._2), e.map(_._1)) else null


  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    try {
      val refs = children.map { child ⇒
        val src = RasterSourceType.deserialize(child.eval(input))
        val srcRE = src.rasterExtent
        subtileDims.map(dims => {
          val subGB = src.layoutBounds(dims)
          val subs = subGB.map(gb => (gb, srcRE.extentFor(gb, clamp = true)))

          subs.map(p => bandIndexes.map(band2ref(src, Some(p))))
        })
          .getOrElse(Seq(bandIndexes.map(band2ref(src, None))))
      }
      refs.transpose.map(ts ⇒ InternalRow(ts.flatMap(_.map(_.toInternalRow)): _*))
    }
    catch {
      case NonFatal(ex) ⇒
        val description = "Error fetching data for one of: " +
          Try(children.map(c => RasterSourceType.deserialize(c.eval(input))))
            .toOption.toSeq.flatten.mkString(", ")
        throw new java.lang.IllegalArgumentException(description, ex)
    }
  }
}

object RasterSourceToRasterRefs {
  def apply(rrs: Column*): TypedColumn[Any, RasterRef] = apply(None, Seq(0), rrs: _*)
  def apply(subtileDims: Option[TileDimensions], bandIndexes: Seq[Int], rrs: Column*): TypedColumn[Any, RasterRef] =
    new Column(new RasterSourceToRasterRefs(rrs.map(_.expr), bandIndexes, subtileDims)).as[RasterRef]

  private[rasterframes] def bandNames(basename: String, bandIndexes: Seq[Int]): Seq[String] = bandIndexes match {
    case Seq() => Seq.empty
    case Seq(0) => Seq(basename)
    case s => s.map(n => basename + "_b" + n)
  }
}
