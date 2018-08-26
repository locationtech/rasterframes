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
import com.vividsolutions.jts.geom.{Geometry, Polygon}
import geotrellis.vector.Extent
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.jts.JTSTypes
import org.apache.spark.sql.types.DataType

/**
 * Catalyst Expression for converting a bounding box structure into a JTS Geometry type.
 *
 * @since 8/24/18
 */
case class BoundsToGeometryExpression(child: Expression) extends UnaryExpression with CodegenFallback {
  private val envEnc = StandardEncoders.envelopeEncoder.resolveAndBind()
  private val extEnc = StandardEncoders.extentEncoder.resolveAndBind()
  private val geomEnc = ExpressionEncoder[Geometry]()

  override def nodeName: String = "boundsGeometry"

  override def toString(): String = s"boundsGeometry($child)"

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
      val env = envEnc.fromRow(r)
      Extent(env)
    }
    else {
      extEnc.fromRow(r)
    }

    geomEnc.toRow(extent.jtsGeom)
  }
}
