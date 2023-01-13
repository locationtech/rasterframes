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

import geotrellis.raster.Tile
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.types.DataType
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.encoders.syntax._

import scala.reflect.runtime.universe._

/** Mixin providing boilerplate for DeclarativeAggrates over tile-conforming columns. */
trait UnaryRasterAggregate extends DeclarativeAggregate {
  def child: Expression

  def nullable: Boolean = child.nullable

  def children: Seq[Expression] = Seq(child)

  protected def tileOpAsExpression[R: TypeTag](name: String, op: Tile => R): Expression => ScalaUDF =
    udfiexpr[R, Any](name, (dataType: DataType) => (a: Any) => if(a == null) null.asInstanceOf[R] else op(UnaryRasterAggregate.extractTileFromAny(dataType, a)))
}

object UnaryRasterAggregate {
  val extractTileFromAny: (DataType, Any) => Tile = (dt: DataType, row: Any) => row match {
    case t: Tile => t
    case r: Row => r.as[Tile]
    case i: InternalRow => DynamicExtractors.internalRowTileExtractor(dt)(i)._1
    case r => throw new Exception(s"UnaryRasterAggregate.extractFromAny unsupported row: $r")
  }
}
