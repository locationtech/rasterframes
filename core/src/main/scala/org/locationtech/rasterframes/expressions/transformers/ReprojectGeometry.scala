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

package org.locationtech.rasterframes.expressions.transformers

import org.locationtech.rasterframes._
import org.locationtech.rasterframes.encoders.serialized_literal
import org.locationtech.jts.geom.Geometry
import geotrellis.proj4.CRS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckFailure
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.jts.{AbstractGeometryUDT, JTSTypes}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.rasterframes.expressions.DynamicExtractors
import org.locationtech.rasterframes.jts.ReprojectionTransformer
import org.locationtech.rasterframes.model.LazyCRS

@ExpressionDescription(
  usage = "_FUNC_(geom, srcCRS, dstCRS) - Reprojects the given `geom` from `srcCRS` to `dstCRS",
  arguments = """
  Arguments:
    * geom - the geometry column to reproject
    * srcCRS - the CRS of the `geom` column
    * dstCRS - the CRS to project geometry into""",
  examples = """
  Examples:
    > SELECT _FUNC_(geom, srcCRS, dstCRS);
       ..."""
)
case class ReprojectGeometry(geometry: Expression, srcCRS: Expression, dstCRS: Expression) extends Expression
  with CodegenFallback {

  override def nodeName: String = "st_reproject"
  override def dataType: DataType = JTSTypes.GeometryTypeInstance
  override def nullable: Boolean = geometry.nullable || srcCRS.nullable || dstCRS.nullable
  override def children: Seq[Expression] = Seq(geometry, srcCRS, dstCRS)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!geometry.dataType.isInstanceOf[AbstractGeometryUDT[_]])
      TypeCheckFailure(s"Input type '${geometry.dataType}' does not conform to a geometry type.")
    else if(!DynamicExtractors.crsExtractor.isDefinedAt(srcCRS.dataType))
      TypeCheckFailure(s"Input type '${srcCRS.dataType}' cannot be interpreted as a CRS.")
    else if(!DynamicExtractors.crsExtractor.isDefinedAt(dstCRS.dataType))
      TypeCheckFailure(s"Input type '${dstCRS.dataType}' cannot be interpreted as a CRS.")
    else TypeCheckResult.TypeCheckSuccess
  }

  /** Reprojects a geometry column from one CRS to another. */
  val reproject: (Geometry, CRS, CRS) => Geometry =
    (sourceGeom, src, dst) ⇒ {
      val trans = new ReprojectionTransformer(src, dst)
      trans.transform(sourceGeom)
    }

  override def eval(input: InternalRow): Any = {
    val src = DynamicExtractors.crsExtractor(srcCRS.dataType)(srcCRS.eval(input))
    val dst = DynamicExtractors.crsExtractor(dstCRS.dataType)(dstCRS.eval(input))
    (src, dst) match {
      // Optimized pass-through case.
      case (s: LazyCRS, r: LazyCRS) if s.encoded == r.encoded => geometry.eval(input)
      case _ =>
        if (geometry.eval(input) != null) {
          val geom = JTSTypes.GeometryTypeInstance.deserialize(geometry.eval(input))
          JTSTypes.GeometryTypeInstance.serialize(reproject(geom, src, dst))
        }
        else {
          null
        }
    }
  }
}

object ReprojectGeometry {
  def apply(geometry: Column, srcCRS: Column, dstCRS: Column): TypedColumn[Any, Geometry] =
    new Column(new ReprojectGeometry(geometry.expr, srcCRS.expr, dstCRS.expr)).as[Geometry]
  def apply(geometry: Column, srcCRS: CRS, dstCRS: Column): TypedColumn[Any, Geometry] =
    new Column(new ReprojectGeometry(geometry.expr, serialized_literal(srcCRS).expr, dstCRS.expr)).as[Geometry]
  def apply(geometry: Column, srcCRS: Column, dstCRS: CRS): TypedColumn[Any, Geometry] =
    new Column(new ReprojectGeometry(geometry.expr, srcCRS.expr, serialized_literal(dstCRS).expr)).as[Geometry]
  def apply(geometry: Column, srcCRS: CRS, dstCRS: CRS): TypedColumn[Any, Geometry] =
    new Column(new ReprojectGeometry(geometry.expr, serialized_literal(srcCRS).expr, serialized_literal(dstCRS).expr)).as[Geometry]
}
