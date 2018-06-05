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

package astraea.spark.rasterframes.extensions

import astraea.spark.rasterframes.util._
import astraea.spark.rasterframes.{RasterFrame, StandardColumns}
import com.vividsolutions.jts.geom.{Point, Polygon}
import geotrellis.proj4.LatLng
import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling.MapKeyTransform
import geotrellis.util.MethodExtensions
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{asc, udf ⇒ sparkUdf}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.locationtech.geomesa.curve.Z2SFC

/**
 * RasterFrame extension methods associated with adding spatially descriptive columns.
 *
 * @since 12/15/17
 */
trait RFSpatialColumnMethods extends MethodExtensions[RasterFrame] with StandardColumns {
  import Implicits.{WithDataFrameMethods, WithRasterFrameMethods}
  import org.locationtech.geomesa.spark.jts._

  /** Returns the key-space to map-space coordinate transform. */
  def mapTransform: MapKeyTransform = self.tileLayerMetadata.merge.mapTransform

  private def keyCol2Bounds: Row ⇒ Polygon = {
    val transform = self.sparkSession.sparkContext.broadcast(mapTransform)
    (r: Row) ⇒ transform.value.keyToExtent(SpatialKey(r.getInt(0), r.getInt(1))).jtsGeom
  }

  private def keyCol2LatLng: Row ⇒ (Double, Double) = {
    val transform = self.sparkSession.sparkContext.broadcast(mapTransform)
    val crs = self.tileLayerMetadata.merge.crs
    (r: Row) ⇒ {
      val center = transform.value.keyToExtent(SpatialKey(r.getInt(0), r.getInt(1))).center.reproject(crs, LatLng)
      (center.x, center.y)
    }
  }

  /**
   * Append a column containing the bounds of the row's spatial key.
   * Coordinates are in native CRS.
   * @param colName name of column to append. Defaults to "bounds"
   * @return updated RasterFrame
   */
  def withBounds(colName: String = BOUNDS_COLUMN.columnName): RasterFrame = {
    val key2Bounds = sparkUdf(keyCol2Bounds)
    self.withColumn(colName, key2Bounds(self.spatialKeyColumn)).certify
  }

  /**
   * Append a column containing the center of the row's spatial key.
   * Coordinate is in native CRS.
   * @param colName name of column to append. Defaults to "center"
   * @return updated RasterFrame
   */
  def withCenter(colName: String = CENTER_COLUMN.columnName): RasterFrame = {
    val key2Center = sparkUdf(keyCol2Bounds andThen (_.getCentroid))
    self.withColumn(colName, key2Center(self.spatialKeyColumn).as[Point]).certify
  }

  /**
   * Append a column containing the center of the row's spatial key.
   * Coordinate is in (longitude, latitude) (EPSG:4326).
   * @param colName name of column to append. Defaults to "center"
   * @return updated RasterFrame
   */
  def withCenterLatLng(colName: String = "center"): RasterFrame = {
    val key2Center = sparkUdf(keyCol2LatLng)
    self.withColumn(colName, key2Center(self.spatialKeyColumn).cast(RFSpatialColumnMethods.LngLatStructType)).certify
  }

  /**
   * Appends a spatial index column
   * @param colName name of new column to create. Defaults to `index`
   * @param applyOrdering if true, adds `.orderBy(asc(colName))` to result. Defaults to `true`
   * @return RasterFrame with index column.
   */
  def withSpatialIndex(colName: String = SPATIAL_INDEX_COLUMN.columnName, applyOrdering: Boolean = true): RasterFrame = {
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
