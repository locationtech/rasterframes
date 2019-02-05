/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea, Inc.
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

import astraea.spark.rasterframes.encoders.CatalystSerializer
import astraea.spark.rasterframes.encoders.CatalystSerializer._
import astraea.spark.rasterframes.encoders.StandardEncoders.extentEncoder
import astraea.spark.rasterframes.ref.ProjectedRasterLike
import geotrellis.vector.Extent
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, TypedColumn}

/**
 * Expression to extract the Extent out of a RasterRef or ProjectedRasterTile column.
 *
 * @since 9/10/18
 */
case class GetExtent(child: Expression) extends OnProjectedRasterExpression with CodegenFallback {
  override def dataType: DataType = CatalystSerializer[Extent].schema
  override def nodeName: String = "extent"
  override def eval(prl: ProjectedRasterLike): InternalRow = prl.extent.toInternalRow
}

object GetExtent {
  def apply(col: Column): TypedColumn[Any, Extent] =
    new Column(new GetExtent(col.expr)).as[Extent]
}
