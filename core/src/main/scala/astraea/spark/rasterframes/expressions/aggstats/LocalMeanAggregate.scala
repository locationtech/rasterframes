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

package astraea.spark.rasterframes.expressions.aggstats

import astraea.spark.rasterframes.expressions.UnaryRasterAggregate
import astraea.spark.rasterframes.expressions.localops.{Add => AddTiles, Divide => DivideTiles}
import astraea.spark.rasterframes.expressions.transformers.SetCellType
import geotrellis.raster.Tile
import geotrellis.raster.mapalgebra.local
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, ExpressionDescription, If, IsNull, Literal}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, TypedColumn}
import astraea.spark.rasterframes.TileType

@ExpressionDescription(
  usage = "_FUNC_(tile) - Computes a new tile contining the mean cell values across all tiles in column.",
  note = "All tiles in the column must be the same size."
)
case class LocalMeanAggregate(child: Expression) extends UnaryRasterAggregate {

  override def dataType: DataType = TileType
  override def nodeName: String = "agg_local_mean"

  private lazy val count =
    AttributeReference("count", TileType, true)()
  private lazy val sum =
    AttributeReference("sum", TileType, true)()

  override def aggBufferAttributes: Seq[AttributeReference] = Seq(
    count,
    sum
  )

  private lazy val Defined = tileOpAsExpression("defined_cells", local.Defined.apply)

  override lazy val initialValues: Seq[Expression] = Seq(
    Literal.create(null, TileType),
    Literal.create(null, TileType)
  )
  override lazy val updateExpressions: Seq[Expression] = Seq(
    If(IsNull(count),
      SetCellType(Defined(child), Literal("int32")),
      If(IsNull(child), count, AddTiles(count, Defined(child)))
    ),
    If(IsNull(sum),
      SetCellType(child, Literal("float64")),
      If(IsNull(child), sum, AddTiles(sum, child))
    )
  )
  override val mergeExpressions: Seq[Expression] = Seq(
    AddTiles(count.left, count.right),
    AddTiles(sum.left, sum.right)
  )
  override lazy val evaluateExpression: Expression = DivideTiles(sum, count)
}
object LocalMeanAggregate {
  import astraea.spark.rasterframes.encoders.StandardEncoders.singlebandTileEncoder

  def apply(tile: Column): TypedColumn[Any, Tile] =
    new Column(new LocalMeanAggregate(tile.expr).toAggregateExpression()).as[Tile]

}
