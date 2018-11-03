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

import astraea.spark.rasterframes.ref.ProjectedRasterLike
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.rf._
import org.apache.spark.sql.types.{ShortType, StructField, StructType}

/**
 * Extract a Tile's dimensions
 * @since 12/21/17
 */
case class GetDimensions(child: Expression) extends OnProjectedRasterExpression
  with CodegenFallback {
  override def nodeName: String = "dimensions"

  def dataType = StructType(Seq(
    StructField("cols", ShortType),
    StructField("rows", ShortType)
  ))

  override def eval(prl: ProjectedRasterLike): Any =
    InternalRow(prl.cols.toShort, prl.rows.toShort)
}

object GetDimensions {
  def apply(col: Column): Column =
    new GetDimensions(col.expr).asColumn
}
