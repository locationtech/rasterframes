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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression

/**
  * An expression with trhee inputs and one output. The output is by default evaluated to null
  * if any input is evaluated to null. A copy of BinaryExpression modified for three args.
  */
trait TrinaryExpression extends Expression with Serializable {
  def left: Expression
  def middle: Expression
  def right: Expression
  override def children: Seq[Expression] = Seq(left, middle, right)
  override def nullable: Boolean = children.exists(_.nullable)
  override def foldable: Boolean = children.forall(_.foldable)
  override def eval(input: InternalRow): Any = {
    val value1 = left.eval(input)
    if (value1 == null) {
      null
    } else {
      val value2 = middle.eval(input)
      if (value2 == null) {
        null
      } else {
        val value3 = right.eval(input)
        if (value3 == null)
          null
        else
          nullSafeEval(value1, value2, value3)
      }
    }
  }
  protected def nullSafeEval(leftInput: Any, middleInput: Any, rightInput: Any): Any
}
