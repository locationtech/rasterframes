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
import org.apache.spark.sql.{Encoder, Encoders}

import scala.reflect.runtime.universe._

/**
 * Implicit encoder definitions for RasterFrame types.
 */
trait StandardEncoders {
  implicit val spatialKeyEncoder = ExpressionEncoder[SpatialKey]
  implicit val temporalKeyEncoder = ExpressionEncoder[TemporalKey]
  implicit val spaceTimeKeyEncoder = ExpressionEncoder[SpaceTimeKey]
  implicit val statsEncoder = ExpressionEncoder[CellStatistics]
  implicit val histEncoder = ExpressionEncoder[CellHistogram]
  implicit val layoutDefinitionEncoder = ExpressionEncoder[LayoutDefinition]
  implicit val stkBoundsEncoder = ExpressionEncoder[KeyBounds[SpaceTimeKey]]
  implicit val extentEncoder = ExpressionEncoder[Extent]

  implicit def singlebandTileEncoder = ExpressionEncoder[Tile]()
  implicit def tileLayerMetadataEncoder[K: TypeTag]: Encoder[TileLayerMetadata[K]] = TileLayerMetadataEncoder[K]()
  implicit val crsEncoder = CRSEncoder()
  implicit val projectedExtentEncoder = ProjectedExtentEncoder()
  implicit val temporalProjectedExtentEncoder = TemporalProjectedExtentEncoder()
  implicit val cellTypeEncoder = CellTypeEncoder()
  implicit val uriEncoder = URIEncoder()
  implicit val envelopeEncoder = EnvelopeEncoder()
}

object StandardEncoders extends StandardEncoders
