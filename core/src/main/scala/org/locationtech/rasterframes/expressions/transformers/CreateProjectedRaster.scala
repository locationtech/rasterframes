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

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, TernaryExpression}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.expressions.DynamicExtractors.tileExtractor
import org.locationtech.rasterframes.expressions.{RasterResult, row}
import org.locationtech.rasterframes.tiles.ProjectedRasterTile
import org.apache.spark.sql.rf.{CrsUDT, TileUDT}
import org.locationtech.rasterframes.encoders.StandardEncoders

@ExpressionDescription(
  usage = "_FUNC_(extent, crs, tile) - Construct a `proj_raster` structure from individual CRS, Extent, and Tile columns",
  arguments = """
  Arguments:
    * extent - extent component of `proj_raster`
    * crs - crs component of `proj_raster`
    * tile - tile component of `proj_raster`"""
)
case class CreateProjectedRaster(tile: Expression, extent: Expression, crs: Expression) extends TernaryExpression with RasterResult with CodegenFallback {
  override def nodeName: String = "rf_proj_raster"

  override def children: Seq[Expression] = Seq(tile, extent, crs)

  override def dataType: DataType = ProjectedRasterTile.prtEncoder.schema

  override def checkInputDataTypes(): TypeCheckResult = (
    if (!tileExtractor.isDefinedAt(tile.dataType)) {
      TypeCheckFailure(s"Column of type '${tile.dataType}' is not or does not have a Tile")
    }
    else if (!extent.dataType.conformsToSchema(StandardEncoders.extentEncoder.schema)) {
      TypeCheckFailure(s"Column of type '${extent.dataType}' is not an Extent")
    }
    else if (!crs.dataType.isInstanceOf[CrsUDT]) {
      TypeCheckFailure(s"Column of type '${crs.dataType}' is not a CRS")
    }
    else TypeCheckSuccess
   )

  private lazy val extentDeser = StandardEncoders.extentEncoder.resolveAndBind().createDeserializer()
  private lazy val crsUdt = new CrsUDT
  private lazy val tileUdt = new TileUDT
  override protected def nullSafeEval(tileInput: Any, extentInput: Any, crsInput: Any): Any = {
    val e = extentDeser.apply(row(extentInput))
    val c = crsUdt.deserialize(crsInput)
    val t = tileUdt.deserialize(tileInput)
    val prt = ProjectedRasterTile(t, e, c)
    toInternalRow(prt)
  }
}

object CreateProjectedRaster {
  def apply(tile: Column, extent: Column, crs: Column): TypedColumn[Any, ProjectedRasterTile] =
    new Column(new CreateProjectedRaster(tile.expr, extent.expr, crs.expr)).as[ProjectedRasterTile]
}
