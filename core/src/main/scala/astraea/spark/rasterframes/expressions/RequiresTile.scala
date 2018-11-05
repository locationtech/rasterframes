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

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.rf.TileUDT

/**
 * Mixin for indicating an expression requires a Tile for input.
 *
 * @since 12/28/17
 */
trait RequiresTile { self: UnaryExpression ⇒
  abstract override def checkInputDataTypes(): TypeCheckResult = RequiresTile.check(child)
}

object RequiresTile {
  def check(expr: Expression): TypeCheckResult =
    if(expr.dataType.isInstanceOf[TileUDT]) TypeCheckSuccess
     else TypeCheckFailure(
       s"Expected 'TileUDT' but received '${expr.dataType.simpleString}'"
     )
}
