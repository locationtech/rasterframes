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

import geotrellis.proj4.LatLng
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription}
import org.apache.spark.sql.types.{DataType, LongType}
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.rasterframes.encoders.SparkBasicEncoders._
import org.locationtech.rasterframes.expressions.DynamicExtractors._
import org.locationtech.rasterframes.expressions.accessors.GetCRS
import org.locationtech.rasterframes.jts.ReprojectionTransformer

/**
  * Constructs a Z2 index in WGS84 from either a Geometry, Extent, ProjectedRasterTile, or RasterSource. First the
  * native extent  is extracted or computed, and then center is used as the indexing location.
  * This function is useful for [range partitioning](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=registerjava#pyspark.sql.DataFrame.repartitionByRange).
  * Also see: https://www.geomesa.org/documentation/user/datastores/index_overview.html
  *
  * @param left geometry-like column
  * @param right CRS column
  * @param indexResolution resolution level of the space filling curve -
  *                        i.e. how many times the space will be recursively quartered
  *                        1-31 is typical.
  */
@ExpressionDescription(
  usage = "_FUNC_(geom, crs) - Constructs a Z2 index in WGS84/EPSG:4326",
  arguments = """
  Arguments:
    * geom - Geometry or item with Geometry:  Extent, ProjectedRasterTile, or RasterSource
    * crs - the native CRS of the `geom` column
"""
)
case class Z2Indexer(left: Expression, right: Expression, indexResolution: Short) extends BinaryExpression with CodegenFallback {

  override def nodeName: String = "rf_z2_index"

  def dataType: DataType = LongType

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!centroidExtractor.isDefinedAt(left.dataType))
      TypeCheckFailure(s"Input type '${left.dataType}' does not look like something with a centroid.")
    else if(!crsExtractor.isDefinedAt(right.dataType))
      TypeCheckFailure(s"Input type '${right.dataType}' does not look like a CRS or something with one.")
    else TypeCheckSuccess
  }

  private lazy val indexer = new Z2SFC(indexResolution)

  override protected def nullSafeEval(leftInput: Any, rightInput: Any): Any = {
    val crs = crsExtractor(right.dataType)(rightInput)
    val coord = centroidExtractor(left.dataType)(leftInput)

    val pt = if(crs == LatLng) coord
    else {
      val trans = new ReprojectionTransformer(crs, LatLng)
      trans(coord)
    }

    indexer.index(pt.getX, pt.getY, lenient = true)
  }

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    copy(newLeft, newRight)
}

object Z2Indexer {
  def apply(targetExtent: Column, targetCRS: Column, indexResolution: Short): TypedColumn[Any, Long] =
    new Column(new Z2Indexer(targetExtent.expr, targetCRS.expr, indexResolution)).as[Long]
  def apply(targetExtent: Column, targetCRS: Column): TypedColumn[Any, Long] =
    new Column(new Z2Indexer(targetExtent.expr, targetCRS.expr, 31)).as[Long]
  def apply(targetExtent: Column, indexResolution: Short = 31): TypedColumn[Any, Long] =
    new Column(new Z2Indexer(targetExtent.expr, GetCRS(targetExtent.expr), indexResolution)).as[Long]
}
