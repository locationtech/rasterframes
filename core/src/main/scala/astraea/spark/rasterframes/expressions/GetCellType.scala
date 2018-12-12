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

import astraea.spark.rasterframes.encoders.CatalystSerializer
import astraea.spark.rasterframes.encoders.CatalystSerializer._
import geotrellis.raster.{CellGrid, CellType}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, TypedColumn}

/**
 * Extract a Tile's cell type
 * @since 12/21/17
 */
case class GetCellType(child: Expression) extends OnCellGridExpression with CodegenFallback {

  override def nodeName: String = "cell_type"

  def dataType: DataType = CatalystSerializer[CellType].schema
  /** Implemented by subtypes to process incoming ProjectedRasterLike entity. */
  override def eval(cg: CellGrid): Any = cg.cellType.toInternalRow
}

object GetCellType {
  import astraea.spark.rasterframes.encoders.StandardEncoders._
  def apply(col: Column): TypedColumn[Any, CellType] =
    new Column(new GetCellType(col.expr)).as[CellType]
}
