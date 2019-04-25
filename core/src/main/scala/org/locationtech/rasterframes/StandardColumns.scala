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

package org.locationtech.rasterframes

import java.sql.Timestamp

import geotrellis.proj4.CRS
import geotrellis.raster.Tile
import geotrellis.spark.{SpatialKey, TemporalKey}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.sql.functions.col
import org.locationtech.jts.geom.{Point => jtsPoint, Polygon => jtsPolygon}
import org.locationtech.rasterframes.encoders.StandardEncoders.PrimitiveEncoders._

/**
 * Constants identifying column in most RasterFrames.
 *
 * @since 2/19/18
 */
trait StandardColumns {
  /** Default RasterFrame spatial column name. */
  val SPATIAL_KEY_COLUMN = col("spatial_key").as[SpatialKey]

  /** Default RasterFrame temporal column name. */
  val TEMPORAL_KEY_COLUMN = col("temporal_key").as[TemporalKey]

  /** Default RasterFrame timestamp column name */
  val TIMESTAMP_COLUMN = col("timestamp").as[Timestamp]

  /** Default RasterFrame column name for an tile extent as geometry value. */
  // This is a `def` because `PolygonUDT` needs to be initialized first.
  def GEOMETRY_COLUMN = col("geometry").as[jtsPolygon]

  /** Default RasterFrame column name for the center coordinates of the tile's bounds. */
  // This is a `def` because `PointUDT` needs to be initialized first.
  def CENTER_COLUMN = col("center").as[jtsPoint]

  /** Default Extent column name. */
  def EXTENT_COLUMN = col("extent").as[Extent]

  /** Default ProjectedExtent column name. */
  def PROJECTED_EXTENT_COLUMN = col("proj_extent").as[ProjectedExtent]

  /** Default CRS column name. */
  def CRS_COLUMN = col("crs").as[CRS]

  /** Default RasterFrame column name for an added spatial index. */
  val SPATIAL_INDEX_COLUMN = col("spatial_index").as[Long]

  /** Default RasterFrame tile column name. */
  // This is a `def` because `TileUDT` needs to be initialized first.
  def TILE_COLUMN = col("tile").as[Tile]

  /** Default RasterFrame `TileFeature.data` column name. */
  val TILE_FEATURE_DATA_COLUMN = col("tile_data")

  /** Default GeoTiff tags column. */
  val METADATA_COLUMN = col("metadata").as[Map[String, String]]

  /** Default column index column for the cells of exploded tiles. */
  val COLUMN_INDEX_COLUMN = col("column_index").as[Int]

  /** Default teil column index column for the cells of exploded tiles. */
  val ROW_INDEX_COLUMN = col("row_index").as[Int]

  /** URI/URL/S3 path to raster. */
  val PATH_COLUMN = col("path").as[String]
}

object StandardColumns extends StandardColumns
