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
import geotrellis.vector.Extent
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.jts.{AbstractGeometryUDT, JTSTypes}
import org.apache.spark.sql.rf._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, TypedColumn}

/**
 * Catalyst Expression for getting the extent of a geometry.
 *
 * @since 8/24/18
 */
case class GeometryToBounds(child: Expression) extends UnaryExpression with CodegenFallback {
  override def nodeName: String = "geometry_bounds"

  override def dataType: DataType = CatalystSerializer[Extent].schema

  override def checkInputDataTypes(): TypeCheckResult = {
    child.dataType match {
      case _: AbstractGeometryUDT[_] ⇒ TypeCheckSuccess
      case o ⇒ TypeCheckFailure(
        s"Expected geometry but received '${o.simpleString}'."
      )
    }
  }

  override protected def nullSafeEval(input: Any): Any = {
    val geom = JTSTypes.GeometryTypeInstance.deserialize(input)
    val extent = Extent(geom.getEnvelopeInternal)
    CatalystSerializer[Extent].toRow(extent)
  }
}

object GeometryToBounds {
  import astraea.spark.rasterframes.encoders.StandardEncoders._

  def apply(bounds: Column): TypedColumn[Any, Extent] =
    new GeometryToBounds(bounds.expr).asColumn.as[Extent]
}