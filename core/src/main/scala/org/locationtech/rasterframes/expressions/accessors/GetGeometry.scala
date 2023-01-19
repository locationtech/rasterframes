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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.jts.JTSTypes
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.jts.geom.Geometry
import org.locationtech.rasterframes.encoders.StandardEncoders.jtsGeometryEncoder
import org.locationtech.rasterframes.expressions.OnTileContextExpression
import org.locationtech.rasterframes.model.TileContext

/**
  * Expression to extract the Extent out of a RasterSource or ProjectedRasterTile column.
  *
  * @since 9/10/18
  */
@ExpressionDescription(
  usage = "_FUNC_(raster) - Fetches the extent (bounding box or envelope) of a ProjectedRasterTile or RasterSource.",
  examples = """
    Examples:
      > SELECT _FUNC_(raster);
         ....
  """)
case class GetGeometry(child: Expression) extends OnTileContextExpression with CodegenFallback {
  def dataType: DataType = JTSTypes.GeometryTypeInstance
  override def nodeName: String = "rf_geometry"
  def eval(ctx: TileContext): InternalRow =
    JTSTypes.GeometryTypeInstance.serialize(ctx.extent.toPolygon())

  def withNewChildInternal(newChild: Expression): Expression = copy(newChild)
}

object GetGeometry {
  def apply(col: Column): TypedColumn[Any, Geometry] =
    new Column(GetGeometry(col.expr)).as[Geometry]
}