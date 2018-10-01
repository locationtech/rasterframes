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

import astraea.spark.rasterframes.encoders.StandardEncoders.crsEncoder
import astraea.spark.rasterframes.tiles.ProjectedRasterTile
import geotrellis.proj4.CRS
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.rf._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, TypedColumn}

/**
 * Expression to extract the CRS out of a RasterRef or ProjectedRasterTile column.
 *
 * @since 9/9/18
 */
case class GetCRS(child: Expression) extends UnaryExpression with CodegenFallback {
  override def dataType: DataType = crsEncoder.schema

  override def nodeName: String = "crs"

  override protected def nullSafeEval(input: Any): Any = {
    child.dataType match {
      case rr: RasterRefUDT ⇒
        val ref = rr.deserialize(input)
        crsEncoder.encode(ref.crs)
      case t: TileUDT ⇒
        val tile = t.deserialize(input)
        tile match {
          case pr: ProjectedRasterTile ⇒
            crsEncoder.encode(pr.crs)
          case _ ⇒ null
        }
    }
  }
}

object GetCRS {
  def apply(ref: Column): TypedColumn[Any, CRS] =
    new GetCRS(ref.expr).asColumn.as[CRS]
}
