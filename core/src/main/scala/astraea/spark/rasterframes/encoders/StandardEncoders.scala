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

import astraea.spark.rasterframes.stats.{CellHistogram, CellStatistics}
import geotrellis.raster.Tile
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.spark.{KeyBounds, SpaceTimeKey, SpatialKey, TemporalKey, TileLayerMetadata}
import geotrellis.vector.Extent
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.reflect.runtime.universe._

/**
 * Implicit encoder definitions for RasterFrame types.
 */
trait StandardEncoders {
  implicit def spatialKeyEncoder = ExpressionEncoder[SpatialKey]
  implicit def temporalKeyEncoder = ExpressionEncoder[TemporalKey]
  implicit def spaceTimeKeyEncoder = ExpressionEncoder[SpaceTimeKey]
  implicit def statsEncoder = ExpressionEncoder[CellStatistics]
  implicit def histEncoder = ExpressionEncoder[CellHistogram]
  implicit def layoutDefinitionEncoder = ExpressionEncoder[LayoutDefinition]
  implicit def stkBoundsEncoder = ExpressionEncoder[KeyBounds[SpaceTimeKey]]
  implicit def extentEncoder = ExpressionEncoder[Extent]

  implicit def singlebandTileEncoder = ExpressionEncoder[Tile]()
  implicit def tileLayerMetadataEncoder[K: TypeTag]: ExpressionEncoder[TileLayerMetadata[K]] = TileLayerMetadataEncoder[K]()
  implicit def crsEncoder = CRSEncoder()
  implicit def projectedExtentEncoder = ProjectedExtentEncoder()
  implicit def temporalProjectedExtentEncoder = TemporalProjectedExtentEncoder()
  implicit def cellTypeEncoder = CellTypeEncoder()
  implicit def uriEncoder = URIEncoder()
  implicit def envelopeEncoder = EnvelopeEncoder()
}

object StandardEncoders extends StandardEncoders
