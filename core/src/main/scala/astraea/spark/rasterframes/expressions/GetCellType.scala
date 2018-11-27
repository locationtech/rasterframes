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

import astraea.spark.rasterframes.encoders.CatalystSerializer._
import geotrellis.raster.Tile
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.rf._
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.sql.{Column, TypedColumn}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Extract a Tile's cell type
 * @since 12/21/17
 */
case class GetCellType(child: Expression) extends UnaryExpression
  with RequiresTile with CodegenFallback {

  override def nodeName: String = "cell_type"

  def dataType: DataType = StringType

  override protected def nullSafeEval(input: Any): Any = {
    val tile = row(input).to[Tile]
    UTF8String.fromString(tile.cellType.name)
  }
}

object GetCellType {
  import astraea.spark.rasterframes.encoders.SparkDefaultEncoders._
  def apply(col: Column): TypedColumn[Any, String] =
    new GetCellType(col.expr).asColumn.as[String]
}
