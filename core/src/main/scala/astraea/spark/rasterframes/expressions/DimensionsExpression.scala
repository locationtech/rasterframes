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
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.rf.TileUDT
import org.apache.spark.sql.types.{ShortType, StructField, StructType}

/**
 * Extract a Tile's dimensions
 * @since 12/21/17
 */
case class DimensionsExpression(child: Expression) extends UnaryExpression
  with RequiresTile with CodegenFallback {
  override def nodeName: String = "dimensions"

  def dataType = StructType(Seq(
    StructField("cols", ShortType),
    StructField("rows", ShortType)
  ))

  override protected def nullSafeEval(input: Any): Any = {
    val r = TileUDT.decode(row(input))
    InternalRow(r.cols.toShort, r.rows.toShort)
  }

}
