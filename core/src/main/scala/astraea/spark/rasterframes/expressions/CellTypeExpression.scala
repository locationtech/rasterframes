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

import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.rf.InternalRowTile.C.CELL_TYPE
import org.apache.spark.sql.types.{DataType, StringType}

/**
 * Extract a Tile's cell type
 * @since 12/21/17
 */
case class CellTypeExpression(child: Expression) extends UnaryExpression with RequiresTile {
  override def toString: String = s"cellType($child)"

  def dataType: DataType = StringType

  override protected def nullSafeEval(input: Any): Any =
    row(input).getUTF8String(CELL_TYPE)

  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, c ⇒ s"$c.getUTF8String($CELL_TYPE);")
}
