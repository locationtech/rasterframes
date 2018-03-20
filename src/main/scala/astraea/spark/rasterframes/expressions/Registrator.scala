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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.gt

/**
 *
 * @author sfitch 
 * @since 10/11/17
 */
private[rasterframes] object Registrator {
  /** Unary expression builder builder. */
  private def ub[A, B](f: A ⇒ B)(a: Seq[A]) = f(a.head)

  def register(sqlContext: SQLContext): Unit = {
    // Expression-oriented functions have a different registration scheme
    // Currently have to register with the `builtin` registry due to Spark data hiding.
    val registry: FunctionRegistry = gt.registry(sqlContext)

    registry.registerFunction("rf_explodeTiles", ExplodeTileExpression.apply(1.0, _))
    registry.registerFunction("rf_cellType", ub(CellType.apply))
    registry.registerFunction("rf_tileDimensions", ub(Dimensions.apply))
  }
}
