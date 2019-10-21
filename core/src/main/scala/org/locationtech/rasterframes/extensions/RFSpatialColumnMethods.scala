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

package org.locationtech.rasterframes.extensions

import org.locationtech.rasterframes.util._
import org.locationtech.rasterframes.RasterFrameLayer
import org.locationtech.jts.geom.Point
import geotrellis.proj4.LatLng
import geotrellis.layer._
import geotrellis.util.MethodExtensions
import geotrellis.vector._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{asc, udf => sparkUdf}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.rasterframes.StandardColumns
import org.locationtech.rasterframes.encoders.serialized_literal

/**
 * RasterFrameLayer extension methods associated with adding spatially descriptive columns.
 *
 * @since 12/15/17
 */
trait RFSpatialColumnMethods extends MethodExtensions[RasterFrameLayer] with StandardColumns {
  import Implicits.{WithDataFrameMethods, WithRasterFrameLayerMethods}
  import org.locationtech.geomesa.spark.jts._

  /** Returns the key-space to map-space coordinate transform. */
  def mapTransform: MapKeyTransform = self.tileLayerMetadata.merge.mapTransform

  private def keyCol2Extent: Row ⇒ Extent = {
    val transform = self.sparkSession.sparkContext.broadcast(mapTransform)
    r ⇒ transform.value.keyToExtent(SpatialKey(r.getInt(0), r.getInt(1)))
  }

  private def keyCol2LatLng: Row ⇒ (Double, Double) = {
    val transform = self.sparkSession.sparkContext.broadcast(mapTransform)
    val crs = self.tileLayerMetadata.merge.crs
    r ⇒ {
      val center = transform.value.keyToExtent(SpatialKey(r.getInt(0), r.getInt(1))).center.reproject(crs, LatLng)
      (center.x, center.y)
    }
  }

  /**
    * Append a column containing the extent of the row's spatial key.
    * Coordinates are in native CRS.
    * @param colName name of column to append. Defaults to "extent"
    * @return updated RasterFrameLayer
    */
  def withExtent(colName: String = EXTENT_COLUMN.columnName): RasterFrameLayer = {
    val key2Extent = sparkUdf(keyCol2Extent)
    self.withColumn(colName, key2Extent(self.spatialKeyColumn)).certify
  }
  /**
    * Append a column containing the CRS of the layer.
    *
    * @param colName name of column to append. Defaults to "crs"
    * @return updated RasterFrameLayer
    */
  def withCRS(colName: String = CRS_COLUMN.columnName): RasterFrameLayer = {
    self.withColumn(colName, serialized_literal(self.crs)).certify
  }

  /**
   * Append a column containing the bounds of the row's spatial key.
   * Coordinates are in native CRS.
   * @param colName name of column to append. Defaults to "geometry"
   * @return updated RasterFrameLayer
   */
  def withGeometry(colName: String = GEOMETRY_COLUMN.columnName): RasterFrameLayer = {
    val key2Bounds = sparkUdf(keyCol2Extent andThen (_.toPolygon()))
    self.withColumn(colName, key2Bounds(self.spatialKeyColumn)).certify
  }

  /**
   * Append a column containing the center of the row's spatial key.
   * Coordinate is in native CRS.
   * @param colName name of column to append. Defaults to "center"
   * @return updated RasterFrameLayer
   */
  def withCenter(colName: String = CENTER_COLUMN.columnName): RasterFrameLayer = {
    val key2Center = sparkUdf(keyCol2Extent andThen (_.center))
    self.withColumn(colName, key2Center(self.spatialKeyColumn).as[Point]).certify
  }

  /**
   * Append a column containing the center of the row's spatial key.
   * Coordinate is in (longitude, latitude) (EPSG:4326).
   * @param colName name of column to append. Defaults to "center"
   * @return updated RasterFrameLayer
   */
  def withCenterLatLng(colName: String = "center"): RasterFrameLayer = {
    val key2Center = sparkUdf(keyCol2LatLng)
    self.withColumn(colName, key2Center(self.spatialKeyColumn).cast(RFSpatialColumnMethods.LngLatStructType)).certify
  }

  /**
   * Appends a spatial index column
   * @param colName name of new column to create. Defaults to `index`
   * @param applyOrdering if true, adds `.orderBy(asc(colName))` to result. Defaults to `true`
   * @return RasterFrameLayer with index column.
   */
  def withSpatialIndex(colName: String = SPATIAL_INDEX_COLUMN.columnName, applyOrdering: Boolean = true): RasterFrameLayer = {
    val zindex = sparkUdf(keyCol2LatLng andThen (p ⇒ Z2SFC.index(p._1, p._2).z))
    self.withColumn(colName, zindex(self.spatialKeyColumn)) match {
      case rf if applyOrdering ⇒ rf.orderBy(asc(colName)).certify
      case rf ⇒ rf.certify
    }
  }
}

object RFSpatialColumnMethods {
  private[rasterframes] val LngLatStructType = StructType(Seq(StructField("longitude", DoubleType), StructField("latitude", DoubleType)))
}
