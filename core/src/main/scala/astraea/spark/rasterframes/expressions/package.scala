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

package astraea.spark.rasterframes

import astraea.spark.rasterframes.expressions.accessors._
import astraea.spark.rasterframes.expressions.generators._
import astraea.spark.rasterframes.expressions.mapalgebra.BinaryRasterOp
import astraea.spark.rasterframes.expressions.transformers._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.rf.VersionShims
import org.apache.spark.sql.{SQLContext, rf}

/**
 * Module of Catalyst expressions for efficiently working with tiles.
 *
 * @since 10/10/17
 */
package object expressions {
  private[expressions] def row(input: Any) = input.asInstanceOf[InternalRow]

  /** Unary expression builder builder. */
  private def ub[A, B](f: A ⇒ B)(a: Seq[A]): B = f(a.head)
  /** Binary expression builder builder. */
  private def bb[A, B](f: (A, A) ⇒ B)(a: Seq[A]): B = f(a.head, a.last)

  def register(sqlContext: SQLContext): Unit = {
    // Expression-oriented functions have a different registration scheme
    // Currently have to register with the `builtin` registry due to Spark data hiding.
    val registry: FunctionRegistry = rf.registry(sqlContext)
    VersionShims.registerExpression(registry, "rf_explode_tiles", ExplodeTiles.apply(1.0, None, _))
    VersionShims.registerExpression(registry, "rf_cell_type", ub(GetCellType.apply))
    VersionShims.registerExpression(registry, "rf_convert_cell_type", bb(SetCellType.apply))
    VersionShims.registerExpression(registry, "rf_tile_dimensions", ub(GetDimensions.apply))
    VersionShims.registerExpression(registry, "rf_bounds_geometry", ub(BoundsToGeometry.apply))
    VersionShims.registerExpression(registry, "rf_local_add", bb(BinaryRasterOp.Add))
    VersionShims.registerExpression(registry, "rf_local_subtract", bb(BinaryRasterOp.Subtract))
    VersionShims.registerExpression(registry, "rf_local_multiply", bb(BinaryRasterOp.Multiply))
    VersionShims.registerExpression(registry, "rf_local_divide", bb(BinaryRasterOp.Divide))
  }
}
