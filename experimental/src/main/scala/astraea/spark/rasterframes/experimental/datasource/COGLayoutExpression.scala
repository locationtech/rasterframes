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

package astraea.spark.rasterframes.experimental.datasource

import astraea.spark.rasterframes.expressions.RequiresTile
import astraea.spark.rasterframes.ref.RasterSource.COGRasterSource
import astraea.spark.rasterframes.tiles.DelayedReadTile
import geotrellis.raster.TileLayout
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.gt.types.TileUDT
import org.apache.spark.sql.types.DataType

/**
 * Extractrs the COG TileLayout from a RasterRefs encoded in a Tile column.
 *
 * @since 9/3/18
 */
case class COGLayoutExpression(child: Expression) extends UnaryExpression with RequiresTile with CodegenFallback {

  override def nodeName: String = s"cog_layout"

  private val tlEncoder = ExpressionEncoder[TileLayout]().resolveAndBind()
  override def dataType: DataType = tlEncoder.schema

  override protected def nullSafeEval(input: Any): Any = {
    val encodedTile = input.asInstanceOf[InternalRow]
    TileUDT.decode(encodedTile) match {
      case DelayedReadTile(_, src: COGRasterSource) ⇒
        src.layout.map(tlEncoder.toRow).orNull
      case _ ⇒ null
    }
  }
}
