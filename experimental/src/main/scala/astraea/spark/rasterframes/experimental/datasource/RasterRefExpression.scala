/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea. Inc.
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
 *
 */

package astraea.spark.rasterframes.experimental.datasource

import java.net.URI

import astraea.spark.rasterframes.ref.RasterSource
import astraea.spark.rasterframes.tiles.DelayedReadTile
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, Generator}
import org.apache.spark.sql.gt.types.TileUDT
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.util.control.NonFatal

/**
 * Catalyst generator to convert a geotiff download URL into a series of rows containing references to the internal tiles and associated extents.
 *
 * @since 5/4/18
 */
case class RasterRefExpression(override val children: Seq[Expression], useTiling: Boolean) extends Expression
  with Generator with CodegenFallback with LazyLogging {

  private val udt = new TileUDT

  override def toString: String = s"rasterRef(${children.mkString(", ")})"

  override def nodeName: String = "rasterRef"

  override def elementSchema: StructType = StructType(
    children.map(e ⇒ StructField(e.nodeName, udt, true))
  )

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    try {
      val refs = children.map { child ⇒
        val uriString = child.eval(input).asInstanceOf[UTF8String].toString
        val uri = URI.create(uriString)
        udt.serialize(DelayedReadTile(RasterSource(uri)))
      }
      Seq(InternalRow(refs: _*))
    }
    catch {
      case NonFatal(ex) ⇒ logger.error("Error fetching data for " + input, ex)
        Traversable.empty
    }
  }
}
