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

import astraea.spark.rasterframes.encoders.StandardEncoders
import com.vividsolutions.jts.geom.Geometry
import geotrellis.vector.Extent
import org.apache.spark.sql.{Column, TypedColumn}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.jts.JTSTypes
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.rf._
import org.locationtech.geomesa.spark.jts.encoders.SpatialEncoders

/**
 * Catalyst Expression for converting a bounding box structure into a JTS Geometry type.
 *
 * @since 8/24/18
 */
case class BoundsToGeometry(child: Expression) extends UnaryExpression with CodegenFallback {
  private val envEnc = StandardEncoders.envelopeEncoder
  private val extEnc = StandardEncoders.extentEncoder

  override def nodeName: String = "bounds_geometry"

  override def dataType: DataType = JTSTypes.GeometryTypeInstance

  override def checkInputDataTypes(): TypeCheckResult = {
    child.dataType match {
      case dt if dt == envEnc.schema || dt == extEnc.schema ⇒ TypeCheckSuccess
      case o ⇒ TypeCheckFailure(
        s"Expected bounding box of form '${envEnc.schema}' but received '${o.simpleString}'."
      )
    }
  }

  override protected def nullSafeEval(input: Any): Any = {
    val r = row(input)
    val extent = if(child.dataType == envEnc.schema) {
      val env = envEnc.decode(r)
      Extent(env)
    }
    else {
      extEnc.decode(r)
    }
    val geom = extent.jtsGeom
    JTSTypes.GeometryTypeInstance.serialize(geom)
  }
}

object BoundsToGeometry extends SpatialEncoders {
  def apply(bounds: Column): TypedColumn[Any, Geometry] =
    new BoundsToGeometry(bounds.expr).asColumn.as[Geometry]
}
