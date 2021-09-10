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

package org.locationtech.rasterframes.expressions.aggregates

import org.locationtech.rasterframes.encoders.SparkBasicEncoders._
import org.locationtech.rasterframes.expressions.UnaryRasterAggregate
import org.locationtech.rasterframes.expressions.tilestats.{DataCells, NoDataCells}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, _}
import org.apache.spark.sql.types.{LongType, Metadata}
import org.apache.spark.sql.{Column, TypedColumn}

/**
 * Cell count (data or NoData) aggregate function.
 *
 * @since 10/5/17
 * @param isData true if count should be of non-NoData cells, false if count should be of NoData cells.
 */
abstract class CellCountAggregate(isData: Boolean) extends UnaryRasterAggregate {
  private lazy val count =
    AttributeReference("count", LongType, false, Metadata.empty)()

  override lazy val aggBufferAttributes = Seq(
    count
  )

  val initialValues = Seq(Literal(0L))

  private def CellTest: Expression => ScalaUDF =
    if (isData) tileOpAsExpressionNew("rf_data_cells", DataCells.op)
    else tileOpAsExpressionNew("rf_no_data_cells", NoDataCells.op)

  val updateExpressions = Seq(
    If(IsNull(child), count, Add(count, CellTest(child)))
  )

  val mergeExpressions = Seq(
    count.left + count.right
  )

  val evaluateExpression = count

  def dataType = LongType
}

object CellCountAggregate {
  @ExpressionDescription(
    usage = "_FUNC_(tile) - Count the total data (non-no-data) cells in a tile column.",
    arguments = """
  Arguments:
    * tile - tile column to analyze""",
    examples = """
  Examples:
    > SELECT _FUNC_(tile);
       92384753"""
  )
  case class DataCells(child: Expression) extends CellCountAggregate(true) {
    override def nodeName: String = "rf_agg_data_cells"
  }
  object DataCells {
    def apply(tile: Column): TypedColumn[Any, Long] =
      new Column(DataCells(tile.expr).toAggregateExpression()).as[Long]
  }
  @ExpressionDescription(
    usage = "_FUNC_(tile) - Count the total no-data cells in a tile column.",
    arguments = """
  Arguments:
    * tile - tile column to analyze""",
    examples = """
  Examples:
    > SELECT _FUNC_(tile);
       23584"""
  )
  case class NoDataCells(child: Expression) extends CellCountAggregate(false) {
    override def nodeName: String = "rf_agg_no_data_cells"
  }
  object NoDataCells {
    def apply(tile: Column): TypedColumn[Any, Long] =
      new Column(NoDataCells(tile.expr).toAggregateExpression()).as[Long]
  }
}



