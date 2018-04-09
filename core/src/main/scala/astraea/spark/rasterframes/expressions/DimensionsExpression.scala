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

package astraea.spark.rasterframes.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.rf.InternalRowTile.C.{COLS, ROWS}
import org.apache.spark.sql.types.{ShortType, StructField, StructType}

/**
 * Extract a Tile's dimensions
 * @since 12/21/17
 */
case class DimensionsExpression(child: Expression) extends UnaryExpression with RequiresTile {
  override def toString: String = s"dimension($child)"

  override def nodeName: String = "dimension"

  def dataType = StructType(Seq(
    StructField("cols", ShortType),
    StructField("rows", ShortType)
  ))

  override protected def nullSafeEval(input: Any): Any = {
    val r = row(input)
    val cols = r.getShort(COLS)
    val rows = r.getShort(ROWS)
    InternalRow(cols, rows)
  }

  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val cols = ctx.freshName("cols")
    val rows = ctx.freshName("rows")
    nullSafeCodeGen(ctx, ev, eval ⇒
      s"""
           final short $cols = $eval.getShort($COLS);
           final short $rows = $eval.getShort($ROWS);
           ${ev.value} = new GenericInternalRow(new Object[] { $cols, $rows });
         """
    )
  }
}
