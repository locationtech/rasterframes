/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Astraea, Inc.
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
 */

package astraea.spark.rasterframes.functions

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.gt.types.TileUDT
import org.apache.spark.sql.types.{DoubleType, LongType}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.rf._

/**
 * Cell mean aggregate function
 * 
 * @since 10/5/17
 */
case class CellMeanAggregateFunction(child: Expression) extends DeclarativeAggregate {

  override def prettyName: String = "agg_mean"

  private lazy val sum = AttributeReference("sum", DoubleType)()
  private lazy val count = AttributeReference("count", LongType)()
  override lazy val aggBufferAttributes = Seq(sum, count)

  val initialValues = Seq(
    Literal(0.0),
    Literal(0L)
  )

  private val dataCellCounts = udf(dataCells)
  private val sumCells = udf(tileSum)

  val updateExpressions = Seq(
    If(IsNull(child), sum , Add(sum, sumCells(child.asColumn).expr)),
    If(IsNull(child), count, Add(count, dataCellCounts(child.asColumn).expr))
  )

  val mergeExpressions = Seq(
    sum.left + sum.right,
    count.left + count.right
  )

  val evaluateExpression = sum / new Cast(count, DoubleType)

  def inputTypes = Seq(TileUDT)

  def nullable = true

  def dataType = DoubleType

  def children = Seq(child)

}



