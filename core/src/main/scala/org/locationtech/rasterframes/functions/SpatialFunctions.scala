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

package org.locationtech.rasterframes.functions
import geotrellis.proj4.CRS
import geotrellis.vector.Extent
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.jts.geom.Geometry
import org.locationtech.rasterframes.expressions.accessors._
import org.locationtech.rasterframes.expressions.transformers._
import org.locationtech.rasterframes.model.TileDimensions

/** Functions associated with georectification, gridding, vector data, and spatial indexing.  */
trait SpatialFunctions {

  /** Query the number of (cols, rows) in a Tile. */
  def rf_dimensions(col: Column): TypedColumn[Any, TileDimensions] = GetDimensions(col)

  /** Extracts the CRS from a RasterSource or ProjectedRasterTile */
  def rf_crs(col: Column): TypedColumn[Any, CRS] = GetCRS(col)

  /** Extracts the bounding box of a geometry as an Extent */
  def st_extent(col: Column): TypedColumn[Any, Extent] = GeometryToExtent(col)

  /** Extracts the bounding box from a RasterSource or ProjectedRasterTile */
  def rf_extent(col: Column): TypedColumn[Any, Extent] = GetExtent(col)

  /** Convert a bounding box structure to a Geometry type. Intented to support multiple schemas. */
  def st_geometry(extent: Column): TypedColumn[Any, Geometry] = ExtentToGeometry(extent)

  /** Extract the extent of a RasterSource or ProjectedRasterTile as a Geometry type. */
  def rf_geometry(raster: Column): TypedColumn[Any, Geometry] = GetGeometry(raster)

  /** Reproject a column of geometry from one CRS to another.
   * @param sourceGeom Geometry column to reproject
   * @param srcCRS Native CRS of `sourceGeom` as a literal
   * @param dstCRSCol Destination CRS as a column
   */
  def st_reproject(sourceGeom: Column, srcCRS: CRS, dstCRSCol: Column): TypedColumn[Any, Geometry] =
    ReprojectGeometry(sourceGeom, srcCRS, dstCRSCol)

  /** Reproject a column of geometry from one CRS to another.
   * @param sourceGeom Geometry column to reproject
   * @param srcCRSCol Native CRS of `sourceGeom` as a column
   * @param dstCRS Destination CRS as a literal
   */
  def st_reproject(sourceGeom: Column, srcCRSCol: Column, dstCRS: CRS): TypedColumn[Any, Geometry] =
    ReprojectGeometry(sourceGeom, srcCRSCol, dstCRS)

  /** Reproject a column of geometry from one CRS to another.
   * @param sourceGeom Geometry column to reproject
   * @param srcCRS Native CRS of `sourceGeom` as a literal
   * @param dstCRS Destination CRS as a literal
   */
  def st_reproject(sourceGeom: Column, srcCRS: CRS, dstCRS: CRS): TypedColumn[Any, Geometry] =
    ReprojectGeometry(sourceGeom, srcCRS, dstCRS)

  /** Reproject a column of geometry from one CRS to another.
   * @param sourceGeom Geometry column to reproject
   * @param srcCRSCol Native CRS of `sourceGeom` as a column
   * @param dstCRSCol Destination CRS as a column
   */
  def st_reproject(sourceGeom: Column, srcCRSCol: Column, dstCRSCol: Column): TypedColumn[Any, Geometry] =
    ReprojectGeometry(sourceGeom, srcCRSCol, dstCRSCol)

  /** Constructs a XZ2 index in WGS84 from either a Geometry, Extent, ProjectedRasterTile, or RasterSource and its CRS.
   * For details: https://www.geomesa.org/documentation/user/datastores/index_overview.html */
  def rf_xz2_index(targetExtent: Column, targetCRS: Column, indexResolution: Short) = XZ2Indexer(targetExtent, targetCRS, indexResolution)

  /** Constructs a XZ2 index in WGS84 from either a Geometry, Extent, ProjectedRasterTile, or RasterSource and its CRS
   * For details: https://www.geomesa.org/documentation/user/datastores/index_overview.html */
  def rf_xz2_index(targetExtent: Column, targetCRS: Column) = XZ2Indexer(targetExtent, targetCRS, 18: Short)

  /** Constructs a XZ2 index with provided resolution level in WGS84 from either a ProjectedRasterTile or RasterSource.
   * For details: https://www.geomesa.org/documentation/user/datastores/index_overview.html  */
  def rf_xz2_index(targetExtent: Column, indexResolution: Short) = XZ2Indexer(targetExtent, indexResolution)

  /** Constructs a XZ2 index with level 18 resolution in WGS84 from either a ProjectedRasterTile or RasterSource.
   * For details: https://www.geomesa.org/documentation/user/datastores/index_overview.html  */
  def rf_xz2_index(targetExtent: Column) = XZ2Indexer(targetExtent, 18: Short)

  /** Constructs a Z2 index in WGS84 from either a Geometry, Extent, ProjectedRasterTile, or RasterSource and its CRS.
   * First the native extent is extracted or computed, and then center is used as the indexing location.
   * For details: https://www.geomesa.org/documentation/user/datastores/index_overview.html */
  def rf_z2_index(targetExtent: Column, targetCRS: Column, indexResolution: Short) = Z2Indexer(targetExtent, targetCRS, indexResolution)

  /** Constructs a Z2 index with index resolution of 31 in WGS84 from either a Geometry, Extent, ProjectedRasterTile, or RasterSource and its CRS.
   * First the native extent is extracted or computed, and then center is used as the indexing location.
   * For details: https://www.geomesa.org/documentation/user/datastores/index_overview.html */
  def rf_z2_index(targetExtent: Column, targetCRS: Column) = Z2Indexer(targetExtent, targetCRS, 31: Short)

  /** Constructs a Z2 index with the given index resolution in WGS84 from either a ProjectedRasterTile or RasterSource
   * First the native extent is extracted or computed, and then center is used as the indexing location.
   * For details: https://www.geomesa.org/documentation/user/datastores/index_overview.html  */
  def rf_z2_index(targetExtent: Column, indexResolution: Short) = Z2Indexer(targetExtent, indexResolution)

  /** Constructs a Z2 index with index resolution of 31 in WGS84 from either a ProjectedRasterTile or RasterSource
   * First the native extent is extracted or computed, and then center is used as the indexing location.
   * For details: https://www.geomesa.org/documentation/user/datastores/index_overview.html  */
  def rf_z2_index(targetExtent: Column) = Z2Indexer(targetExtent, 31: Short)

}
