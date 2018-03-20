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
 */

package astraea.spark.rasterframes.encoders

import astraea.spark.rasterframes.Statistics
import geotrellis.proj4.CRS
import geotrellis.raster.histogram.Histogram
import geotrellis.raster.{CellType, MultibandTile, Tile}
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.spark.{KeyBounds, SpaceTimeKey, SpatialKey, TemporalKey, TileLayerMetadata}
import geotrellis.vector.Extent
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.reflect.runtime.universe._

/**
 * Implicit encoder definitions for RasterFrame types.
 */
trait EncoderImplicits {
  implicit def singlebandTileEncoder = ExpressionEncoder[Tile]()
  implicit def multibandTileEncoder = ExpressionEncoder[MultibandTile]()
  implicit val crsEncoder = CRSEncoder(): ExpressionEncoder[CRS]
  implicit val extentEncoder = ExpressionEncoder[Extent]()
  implicit val projectedExtentEncoder = ProjectedExtentEncoder()
  implicit val temporalProjectedExtentEncoder = TemporalProjectedExtentEncoder()
  implicit def histogramDoubleEncoder = ExpressionEncoder[Histogram[Double]]()
  implicit val statsEncoder = ExpressionEncoder[Statistics]()
  implicit def tileLayerMetadataEncoder[K: TypeTag]: Encoder[TileLayerMetadata[K]] = TileLayerMetadataEncoder[K]()
  implicit val layoutDefinitionEncoder = ExpressionEncoder[LayoutDefinition]()
  implicit val stkBoundsEncoder = ExpressionEncoder[KeyBounds[SpaceTimeKey]]()
  implicit val cellTypeEncoder = CellTypeEncoder(): ExpressionEncoder[CellType]
  implicit val spatialKeyEncoder = ExpressionEncoder[SpatialKey]()
  implicit val temporalKeyEncoder = ExpressionEncoder[TemporalKey]()
  implicit val spaceTimeKeyEncoder = ExpressionEncoder[SpaceTimeKey]()
}

