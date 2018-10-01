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

import astraea.spark.rasterframes.ref.RasterRef
import astraea.spark.rasterframes.util._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.rf._
import org.apache.spark.sql.types.{StructField, StructType}

import scala.util.control.NonFatal

/**
 * Accepts RasterRef and generates one or more RasterRef instances representing the
 * native internal sub-tiling, if any.
 *
 * @since 9/6/18
 */
case class ExpandNativeTiling(children: Seq[Expression]) extends Expression
  with Generator with CodegenFallback with ExpectsInputTypes with LazyLogging {

  private val rrType = new RasterRefUDT()

  override def inputTypes = Seq.fill(children.size)(rrType)
  override def nodeName: String = "expand_native_tiling"
  override def elementSchema: StructType = StructType(
    children.map(e ⇒ StructField(e.name, rrType, true))
  )

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    try {
      val tiled = children.map { child ⇒
        val ref = RasterRefUDT.decode(row(child.eval(input)))
        RasterRef.tileToNative(ref)
      }
      tiled.transpose.map(ts ⇒ InternalRow(ts.map(RasterRefUDT.encode): _*))
    }
    catch {
      case NonFatal(ex) ⇒
        logger.error("Error fetching data for " + input, ex)
        Traversable.empty
    }
  }
}

object ExpandNativeTiling {
  def apply(rrs: Column*): Column =
    new ExpandNativeTiling(rrs.map(_.expr)).asColumn
}
