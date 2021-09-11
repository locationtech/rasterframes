/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Astraea, Inc.
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

package org.locationtech.rasterframes.encoders

import org.locationtech.rasterframes.stats.{CellHistogram, CellStatistics, LocalCellStatistics}
import org.locationtech.jts.geom.Envelope
import geotrellis.proj4.CRS
import geotrellis.raster.{CellSize, CellType, Dimensions, Raster, Tile, TileLayout}
import geotrellis.layer._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.util.QuantileSummaries
import org.locationtech.geomesa.spark.jts.encoders.SpatialEncoders
import org.locationtech.rasterframes.model.{CellContext, LongExtent, TileContext, TileDataContext}
import frameless.TypedEncoder

import java.net.URI
import java.sql.Timestamp

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * TODO: move this overload to GeoTrellis, the reason is in the generic method invocation and Integral in implicits
 */
object DimensionsInt {
  def apply(cols: Int, rows: Int): Dimensions[Int] = new Dimensions(cols, rows)
}

object EnvelopeLocal {
  def apply(minx: Double, maxx: Double, miny: Double, maxy: Double): Envelope = new Envelope(minx, maxx, miny, miny)
}

/**
 * Implicit encoder definitions for RasterFrameLayer types.
 */
trait StandardEncoders extends SpatialEncoders with TypedEncoders {
  def expressionEncoder[T: TypeTag]: ExpressionEncoder[T] = ExpressionEncoder()

  implicit lazy val crsExpressionEncoder: ExpressionEncoder[CRS] = ExpressionEncoder()
  implicit lazy val projectedExtentEncoder: ExpressionEncoder[ProjectedExtent] = ExpressionEncoder()
  implicit lazy val temporalProjectedExtentEncoder: ExpressionEncoder[TemporalProjectedExtent] = ExpressionEncoder()
  implicit lazy val timestampEncoder: ExpressionEncoder[Timestamp] = ExpressionEncoder()
  implicit lazy val strMapEncoder: ExpressionEncoder[Map[String, String]] = ExpressionEncoder()
  implicit lazy val cellStatsEncoder: ExpressionEncoder[CellStatistics] = ExpressionEncoder()
  implicit lazy val cellHistEncoder: ExpressionEncoder[CellHistogram] = ExpressionEncoder()
  implicit lazy val localCellStatsEncoder: ExpressionEncoder[LocalCellStatistics] = ExpressionEncoder()
  implicit lazy val uriEncoder: ExpressionEncoder[URI]   = typedExpressionEncoder[URI]
  implicit lazy val quantileSummariesEncoder: ExpressionEncoder[QuantileSummaries] = typedExpressionEncoder[QuantileSummaries]

  implicit lazy val envelopeEncoder: ExpressionEncoder[Envelope] = typedExpressionEncoder
  implicit lazy val longExtentEncoder: ExpressionEncoder[LongExtent] = typedExpressionEncoder
  implicit lazy val extentEncoder: ExpressionEncoder[Extent] = typedExpressionEncoder
  implicit lazy val cellSizeEncoder: ExpressionEncoder[CellSize] = typedExpressionEncoder
  implicit lazy val tileLayoutEncoder: ExpressionEncoder[TileLayout] = typedExpressionEncoder
  implicit lazy val spatialKeyEncoder: ExpressionEncoder[SpatialKey] = typedExpressionEncoder
  implicit lazy val temporalKeyEncoder: ExpressionEncoder[TemporalKey] = typedExpressionEncoder
  implicit lazy val spaceTimeKeyEncoder: ExpressionEncoder[SpaceTimeKey] = typedExpressionEncoder
  implicit def keyBoundsEncoder[K: TypedEncoder]: ExpressionEncoder[KeyBounds[K]] = typedExpressionEncoder[KeyBounds[K]]
  implicit def boundsEncoder[K: TypedEncoder]: ExpressionEncoder[Bounds[K]] = keyBoundsEncoder[KeyBounds[K]].asInstanceOf[ExpressionEncoder[Bounds[K]]]
  implicit lazy val cellTypeEncoder: ExpressionEncoder[CellType] = typedExpressionEncoder[CellType]
  implicit lazy val dimensionsEncoder: ExpressionEncoder[Dimensions[Int]] = typedExpressionEncoder
  implicit lazy val layoutDefinitionEncoder: ExpressionEncoder[LayoutDefinition] = typedExpressionEncoder
  implicit def tileLayerMetadataEncoder[K: TypedEncoder: ClassTag]: ExpressionEncoder[TileLayerMetadata[K]] = typedExpressionEncoder[TileLayerMetadata[K]]
  implicit lazy val tileContextEncoder: ExpressionEncoder[TileContext] = typedExpressionEncoder
  implicit lazy val tileDataContextEncoder: ExpressionEncoder[TileDataContext] = typedExpressionEncoder
  implicit lazy val cellContextEncoder: ExpressionEncoder[CellContext] = typedExpressionEncoder

  implicit lazy val tileEncoder: ExpressionEncoder[Tile] = typedExpressionEncoder
  implicit lazy val optionalTileEncoder: ExpressionEncoder[Option[Tile]] = typedExpressionEncoder
  implicit lazy val rasterEncoder: ExpressionEncoder[Raster[Tile]] = typedExpressionEncoder
}

object StandardEncoders extends StandardEncoders
