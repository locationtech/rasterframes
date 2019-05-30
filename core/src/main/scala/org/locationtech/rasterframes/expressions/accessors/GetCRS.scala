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

package org.locationtech.rasterframes.expressions.accessors

import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.encoders.StandardEncoders.crsEncoder
import org.locationtech.rasterframes.expressions.OnTileContextExpression
import geotrellis.proj4.CRS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.rasterframes.model.TileContext

/**
 * Expression to extract the CRS out of a RasterRef or ProjectedRasterTile column.
 *
 * @since 9/9/18
 */
case class GetCRS(child: Expression) extends OnTileContextExpression with CodegenFallback {
  override def dataType: DataType = schemaOf[CRS]
  override def nodeName: String = "rf_crs"
  override def eval(ctx: TileContext): InternalRow = ctx.crs.toInternalRow
}

object GetCRS {
  def apply(ref: Column): TypedColumn[Any, CRS] =
    new Column(new GetCRS(ref.expr)).as[CRS]
}
