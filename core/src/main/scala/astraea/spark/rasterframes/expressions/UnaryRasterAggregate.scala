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
import astraea.spark.rasterframes.expressions.DynamicExtractors.rowTileExtractor
import geotrellis.raster.Tile
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import scala.reflect.runtime.universe._

/** Mixin providing boilerplate for DeclarativeAggrates over tile-conforming columns. */
trait UnaryRasterAggregate extends DeclarativeAggregate {
  def child: Expression

  def nullable = child.nullable

  def children = Seq(child)

  protected def tileOpAsExpression[R: TypeTag](op: Tile => R): Expression => ScalaUDF =
    udfexpr[R, Any]((a: Any) => op(extractTileFromAny(a)))

  protected def extractTileFromAny = (a: Any) => a match {
    case t: Tile => t
    case r: Row => rowTileExtractor(child.dataType)(r)._1
  }
}
