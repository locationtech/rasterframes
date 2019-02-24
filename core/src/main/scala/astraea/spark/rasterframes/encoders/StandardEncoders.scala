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

import java.net.URI

import astraea.spark.rasterframes.stats.CellHistogram
import astraea.spark.rasterframes.tiles.ProjectedRasterTile
import com.vividsolutions.jts.geom.Envelope
import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, Tile}
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.spark.{KeyBounds, SpaceTimeKey, SpatialKey, TemporalKey, TemporalProjectedExtent, TileLayerMetadata}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.locationtech.geomesa.spark.jts.encoders.SpatialEncoders

import scala.reflect.runtime.universe._

/**
 * Implicit encoder definitions for RasterFrame types.
 */
trait StandardEncoders extends SpatialEncoders {
  implicit def spatialKeyEncoder: ExpressionEncoder[SpatialKey] = ExpressionEncoder()
  implicit def temporalKeyEncoder: ExpressionEncoder[TemporalKey] = ExpressionEncoder()
  implicit def spaceTimeKeyEncoder: ExpressionEncoder[SpaceTimeKey] = ExpressionEncoder()
  implicit def histEncoder: ExpressionEncoder[CellHistogram] = ExpressionEncoder()
  implicit def layoutDefinitionEncoder: ExpressionEncoder[LayoutDefinition] = ExpressionEncoder()
  implicit def stkBoundsEncoder: ExpressionEncoder[KeyBounds[SpaceTimeKey]] = ExpressionEncoder()
  implicit def extentEncoder: ExpressionEncoder[Extent] = ExpressionEncoder()
  implicit def singlebandTileEncoder: ExpressionEncoder[Tile] = ExpressionEncoder()
  implicit def tileLayerMetadataEncoder[K: TypeTag]: ExpressionEncoder[TileLayerMetadata[K]] = TileLayerMetadataEncoder()
  implicit def crsEncoder: ExpressionEncoder[CRS] = CRSEncoder()
  implicit def projectedExtentEncoder: ExpressionEncoder[ProjectedExtent] = ProjectedExtentEncoder()
  implicit def temporalProjectedExtentEncoder: ExpressionEncoder[TemporalProjectedExtent] = TemporalProjectedExtentEncoder()
  implicit def cellTypeEncoder: ExpressionEncoder[CellType] = CellTypeEncoder()
  implicit def uriEncoder: ExpressionEncoder[URI] = URIEncoder()
  implicit def envelopeEncoder: ExpressionEncoder[Envelope] = EnvelopeEncoder()
  //implicit def projectedRasterTileEncoder = ProjectedRasterTile.prtEncoder
}

object StandardEncoders extends StandardEncoders
