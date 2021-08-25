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

import geotrellis.proj4.CRS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.rf.CrsUDT
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.encoders.StandardEncoders.crsSparkEncoder
import org.locationtech.rasterframes.expressions.DynamicExtractors._
import org.locationtech.rasterframes.tiles.ProjectedRasterTile
import org.locationtech.rasterframes.{CrsType, RasterSourceType}
import org.apache.spark.sql.rf.RasterSourceUDT
import org.locationtech.rasterframes.ref.RasterRef
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.types.StringType

/**
 * Expression to extract the CRS out of a RasterRef or ProjectedRasterTile column.
 *
 * @since 9/9/18
 */
@ExpressionDescription(
  usage = "_FUNC_(raster) - Fetches the CRS of a ProjectedRasterTile or RasterSource, or converts a proj4 string column.",
  examples = """
    Examples:
      > SELECT _FUNC_(raster);
         ....
  """)
case class GetCRS(child: Expression) extends UnaryExpression with CodegenFallback {
  override def dataType: DataType = new CrsUDT
  override def nodeName: String = "rf_crs"

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!crsExtractor.isDefinedAt(child.dataType) )
      TypeCheckFailure(s"Input type '${child.dataType}' does not conform to a CRS or something with one.")
    else TypeCheckSuccess
  }

  private lazy val crsUdt = new CrsUDT

  override protected def nullSafeEval(input: Any): Any = {
    // TODO: move construction of this function to checkInputDataType as dataType is constant per instance of this exp.
    child.dataType match {
      case _: CrsUDT =>
        input

      case _: StringType =>
        val str = input.asInstanceOf[UTF8String]
        val crs = CrsType.deserialize(str)
        crsUdt.serialize(crs)

      case t if t.conformsToSchema(ProjectedRasterTile.prtEncoder.schema) =>
        val idx = ProjectedRasterTile.prtEncoder.schema.fieldIndex("crs")
        input.asInstanceOf[InternalRow].get(idx, CrsType)

      case _: RasterSourceUDT =>
        val rs = RasterSourceType.deserialize(input)
        val crs = rs.crs
        crsUdt.serialize(crs)

      case t if t.conformsToSchema(RasterRef.rrEncoder.schema) =>
        val row = input.asInstanceOf[InternalRow]
        val idx = RasterRef.rrEncoder.schema.fieldIndex("source")
        val rsc = row.get(idx, RasterSourceType)
        val rs = RasterSourceType.deserialize(rsc)
        val crs = rs.crs
        crsUdt.serialize(crs)
    }
  }

}

object GetCRS {
  def apply(ref: Column): TypedColumn[Any, CRS] =
    new Column(new GetCRS(ref.expr)).as[CRS]
}
