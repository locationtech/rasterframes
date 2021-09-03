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

import geotrellis.layer.SpatialKey
import geotrellis.raster.Tile
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.types.DataType
import org.locationtech.rasterframes.encoders.StandardEncoders
import org.locationtech.rasterframes.expressions.DynamicExtractors.{internalRowTileExtractor, rowTileExtractor}
import org.locationtech.rasterframes.tileLayerMetadataEncoder

import scala.reflect.runtime.universe._

/** Mixin providing boilerplate for DeclarativeAggrates over tile-conforming columns. */
trait UnaryRasterAggregate extends DeclarativeAggregate {
  def child: Expression

  def nullable: Boolean = child.nullable

  def children = Seq(child)

  protected def tileOpAsExpression[R: TypeTag](name: String, op: Tile => R): Expression => ScalaUDF =
    udfexpr[R, Any](name, (a: Any) => if(a == null) null.asInstanceOf[R] else op(extractTileFromAny(a)))

  protected def tileOpAsExpressionNew[R: TypeTag](name: String, op: Tile => R): Expression => ScalaUDF =
    udfexprNew[R, Any](name, (dataType: DataType) => (a: Any) => if(a == null) null.asInstanceOf[R] else op(UnaryRasterAggregate.extractTileFromAny2(dataType, a)))

  protected def tileOpAsExpressionNewUntyped[R: TypeTag](name: String, op: Tile => R): Expression => ScalaUDF =
    udfexprNewUntyped[R, Any](name, (dataType: DataType) => (a: Any) => if(a == null) null.asInstanceOf[R] else op(UnaryRasterAggregate.extractTileFromAny2(dataType, a)))

  protected val extractTileFromAny = (a: Any) => a match {
    case t: Tile => println("HERE1"); t
    case r: Row => println("HERE"); rowTileExtractor(child.dataType)(r)._1
    case null => println("HERENULL"); null
    case _ => println("WTF"); null
  }
}

object UnaryRasterAggregate {
  val extractTileFromAny2: (DataType, Any) => Tile = (dt: DataType, row: Any) => row match {
    case t: Tile => t
    case r: Row =>
      StandardEncoders
        .singlebandTileEncoder
        .resolveAndBind()
        .createDeserializer()(
          RowEncoder(StandardEncoders.singlebandTileEncoder.schema).createSerializer()(r)
        )
    case i: InternalRow =>
      internalRowTileExtractor(dt)(i)._1
    case s => throw new Exception(s"UnaryRasterAggregate.extractFromAny2: ${s}")
  }
}
