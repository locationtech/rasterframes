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
import geotrellis.vector.Extent
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription}
import org.apache.spark.sql.jts.JTSTypes
import org.apache.spark.sql.rf.RasterSourceUDT
import org.apache.spark.sql.types.{DataType, LongType}
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.geomesa.curve.XZ2SFC
import org.locationtech.jts.geom.{Envelope, Geometry, GeometryFactory}
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.expressions.DynamicExtractors._
import org.locationtech.rasterframes.expressions.row
import org.locationtech.rasterframes.jts.ReprojectionTransformer
import org.locationtech.rasterframes.ref.{RasterRef, RasterSource}
import org.locationtech.rasterframes.tiles.ProjectedRasterTile
import org.apache.spark.sql.rf
import org.locationtech.rasterframes.expressions.accessors.GetCRS

/**
  * Constructs a XZ2 index in WGS84 from either a Geometry, Extent, ProjectedRasterTile, or RasterSource
  *
  * @param left geometry-like column
  * @param right CRS column
  * @param indexResolution resolution level of the space filling curve -
  *                        i.e. how many times the space will be recursively quartered
  *                        1-18 is typical.
  */
@ExpressionDescription(
  usage = "_FUNC_(geom, crs) - Constructs a XZ2 index in WGS84/EPSG:4326",
  arguments = """
  Arguments:
    * geom - Geometry or item with Geometry:  Extent, ProjectedRasterTile, or RasterSource
    * crs - the native CRS of the `geom` column
"""
)
case class XZ2Indexer(left: Expression, right: Expression, indexResolution: Short = 18)
  extends BinaryExpression with CodegenFallback {

  override def nodeName: String = "rf_spatial_index"

  override def dataType: DataType = LongType

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!extentLikeExtractor.orElse(projectedRasterLikeExtractor).isDefinedAt(left.dataType))
      TypeCheckFailure(s"Input type '${left.dataType}' does not look like something with an Extent or something with one.")
    else if(!crsExtractor.isDefinedAt(right.dataType))
      TypeCheckFailure(s"Input type '${right.dataType}' does not look like something with a CRS.")
    else TypeCheckSuccess
  }

  private lazy val indexer = XZ2SFC(indexResolution)
  private lazy val gf = new GeometryFactory()

  override protected def nullSafeEval(leftInput: Any, rightInput: Any): Any = {
    val crs = crsExtractor(right.dataType)(rightInput)

    val coords = left.dataType match {
      case t if rf.WithTypeConformity(t).conformsTo(JTSTypes.GeometryTypeInstance) =>
        JTSTypes.GeometryTypeInstance.deserialize(leftInput)
      case t if t.conformsTo[Extent] =>
        row(leftInput).to[Extent]
      case t if t.conformsTo[Envelope] =>
        row(leftInput).to[Envelope]
      case _: RasterSourceUDT ⇒
        row(leftInput).to[RasterSource](RasterSourceUDT.rasterSourceSerializer).extent
      case t if t.conformsTo[ProjectedRasterTile] =>
        row(leftInput).to[ProjectedRasterTile].extent
      case t if t.conformsTo[RasterRef] =>
        row(leftInput).to[RasterRef].extent
    }

    // If no transformation is needed then just normalize to an Envelope
    val env = if(crs == LatLng) coords match {
      case e: Extent => e.jtsEnvelope
      case g: Geometry => g.getEnvelopeInternal
      case e: Envelope => e
    }
    // Otherwise convert to geometry, transform, and get envelope
    else {
      val trans = new ReprojectionTransformer(crs, LatLng)
      coords match {
        case e: Extent => trans(e.jtsGeom).getEnvelopeInternal
        case g: Geometry => trans(g).getEnvelopeInternal
        case e: Envelope => trans(gf.toGeometry(e)).getEnvelopeInternal
      }
    }

    val index = indexer.index(
      env.getMinX, env.getMinY, env.getMaxX, env.getMaxY,
      lenient = false
    )
    index
  }
}

object XZ2Indexer {
  import org.locationtech.rasterframes.encoders.SparkBasicEncoders.longEnc
  def apply(targetExtent: Column, targetCRS: Column): TypedColumn[Any, Long] =
    new Column(new XZ2Indexer(targetExtent.expr, targetCRS.expr)).as[Long]
  def apply(targetExtent: Column): TypedColumn[Any, Long] =
    new Column(new XZ2Indexer(targetExtent.expr, GetCRS(targetExtent.expr))).as[Long]
}
