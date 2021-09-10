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

import geotrellis.layer._
import geotrellis.raster.resample.{NearestNeighbor, ResampleMethod => GTResampleMethod}
import geotrellis.util.MethodExtensions

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.{MetadataBuilder, StructField}
import org.apache.spark.sql.{Column, DataFrame, TypedColumn}
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.encoders._
import org.locationtech.rasterframes.expressions.DynamicExtractors
import org.locationtech.rasterframes.tiles.ProjectedRasterTile
import org.locationtech.rasterframes.util._
import org.locationtech.rasterframes.{MetadataKeys, RasterFrameLayer}
import spray.json.JsonFormat
import org.locationtech.rasterframes.util.JsonCodecs._

import scala.util.Try
import org.apache.spark.sql.rf.CrsUDT

/**
 * Extension methods over [[DataFrame]].
 *
 * @since 7/18/17
 */
trait DataFrameMethods[DF <: DataFrame] extends MethodExtensions[DF] with MetadataKeys {
  import Implicits.{WithDataFrameMethods, WithMetadataBuilderMethods, WithMetadataMethods, WithRasterFrameLayerMethods}

  private def selector(column: Column): Attribute => Boolean = (attr: Attribute) =>
    attr.name == column.columnName || attr.semanticEquals(column.expr)

  /** Map over the Attribute representation of Columns, modifying the one matching `column` with `op`. */
  private[rasterframes] def mapColumnAttribute(column: Column, op: Attribute =>  Attribute): DF = {
    val analyzed = self.queryExecution.analyzed.output
    val selects = selector(column)
    val attrs = analyzed.map { attr =>
      if(selects(attr)) op(attr) else attr
    }
    self.select(attrs.map(a => new Column(a)): _*).asInstanceOf[DF]
  }

  private[rasterframes] def addColumnMetadata(column: Column, op: MetadataBuilder => MetadataBuilder): DF = {
    mapColumnAttribute(column, attr => {
      val md = new MetadataBuilder().withMetadata(attr.metadata)
      attr.withMetadata(op(md).build)
    })
  }

  private[rasterframes] def fetchMetadataValue[D](column: Column, reader: (Attribute) => D): Option[D] = {
    val analyzed = self.queryExecution.analyzed.output
    analyzed.find(selector(column)).map(reader)
  }

  private[rasterframes]
  def setSpatialColumnRole[K: SpatialComponent: JsonFormat](
    column: Column, md: TileLayerMetadata[K]): DF =
    addColumnMetadata(column,
      _.attachContext(md.asColumnMetadata).tagSpatialKey
    )

  private[rasterframes]
  def setTemporalColumnRole(column: Column): DF =
    addColumnMetadata(column, _.tagTemporalKey)

  /** Get the role tag the column plays in the RasterFrameLayer, if any. */
  private[rasterframes]
  def getColumnRole(column: Column): Option[String] =
    fetchMetadataValue(column, _.metadata.getString(SPATIAL_ROLE_KEY))

  /** Get the columns that are of type `Tile` */
  def tileColumns: Seq[Column] =
    self.schema.fields
      .filter(f => DynamicExtractors.tileExtractor.isDefinedAt(f.dataType))
      .map(f => self.col(f.name))

  /** Get the columns that look like `ProjectedRasterTile`s. */
  def projRasterColumns: Seq[Column] =
    self.schema.fields
      .filter(_.dataType.conformsToSchema(ProjectedRasterTile.prtEncoder.schema))
      .map(f => self.col(f.name))

  /** Get the columns that look like `Extent`s. */
  def extentColumns: Seq[Column] =
    self.schema.fields
      .filter(_.dataType.conformsToSchema(extentEncoder.schema))
      .map(f => self.col(f.name))

  /** Get the columns that look like `CRS`s. */
  def crsColumns: Seq[Column] =
    self.schema.fields
      .filter(_.dataType.isInstanceOf[CrsUDT])
      .map(f => self.col(f.name))

  /** Get the columns that are not of type `Tile` */
  def notTileColumns: Seq[Column] =
    self.schema.fields
      .filter(f => !DynamicExtractors.tileExtractor.isDefinedAt(f.dataType))
      .map(f => self.col(f.name))

  /** Get the spatial column. */
  def spatialKeyColumn: Option[TypedColumn[Any, SpatialKey]] = {
    val key = findSpatialKeyField
    key
      .map(_.name)
      .map(self.col(_).as[SpatialKey])
  }

  /** Get the temporal column, if any. */
  def temporalKeyColumn: Option[TypedColumn[Any, TemporalKey]] = {
    val key = findTemporalKeyField
    key.map(_.name).map(self.col(_).as[TemporalKey])
  }

  /** Find the field tagged with the requested `role` */
  private[rasterframes] def findRoleField(role: String): Option[StructField] =
    self.schema.fields.find(
      f =>
        f.metadata.contains(SPATIAL_ROLE_KEY) &&
          f.metadata.getString(SPATIAL_ROLE_KEY) == role
    )

  /** The spatial key is the first one found with context metadata attached to it. */
  private[rasterframes] def findSpatialKeyField: Option[StructField] =
    findRoleField(SPATIAL_KEY_COLUMN.columnName)

  /** The temporal key is the first one found with the temporal tag. */
  private[rasterframes] def findTemporalKeyField: Option[StructField] =
    findRoleField(TEMPORAL_KEY_COLUMN.columnName)

  /** Renames all columns such that they start with the given prefix string.
   * Useful for preparing dataframes for joins where duplicate names may arise.
   */
  def withPrefixedColumnNames(prefix: String): DF =
    self.columns.foldLeft(self)((df, c) => df.withColumnRenamed(c, s"$prefix$c").asInstanceOf[DF])

  /**
    * Performs a jeft join on the dataframe `right` to this one, reprojecting and merging tiles as necessary.
    * The operation is logically a "left outer" join, with the left side also determining the target CRS and extents.
    * Right side may have multiple Tile columns. Assumes both dataframes use the column names `extent` and `crs` for
    * the Extent and CRS details for each row. The join expression used is:
    *
    * {{{
    *   st_intersects(st_geometry(leftExtent), st_reproject(st_geometry(rightExtent), rightCRS, leftCRS))
    * }}}
    *
    * @param right Right side of the join.
    * @param resampleMethod string indicating method to use for resampling.
    * @return joined dataframe
    */
  def rasterJoin(right: DataFrame, resampleMethod: GTResampleMethod = NearestNeighbor): DataFrame = RasterJoin(self, right, resampleMethod, None)

  /**
    * Performs a jeft join on the dataframe `right` to this one, reprojecting and merging tiles as necessary.
    * The operation is logically a "left outer" join, with the left side also determining the target CRS and extents.
    * Right side may have multiple Tile columns. This variant allows for the specific geospatial columns to be
    * specified. The join expression used is:
    * {{{
    *   st_intersects(st_geometry(leftExtent), st_reproject(st_geometry(rightExtent), rightCRS, leftCRS))
    * }}}
    *
    * @param right right dataframe
    * @param leftExtent this (left) dataframe's Extent column
    * @param leftCRS this (left) datafrasme's CRS column
    * @param rightExtent right dataframe's CRS extent
    * @param rightCRS right dataframe's CRS column
    * @param resampleMethod string indicating method to use for resampling.
    * @return joined dataframe
    */
  def rasterJoin(right: DataFrame, leftExtent: Column, leftCRS: Column, rightExtent: Column, rightCRS: Column, resampleMethod: GTResampleMethod): DataFrame =
    RasterJoin(self, right, leftExtent, leftCRS, rightExtent, rightCRS, resampleMethod, None)

  /**
    * Performs a jeft join on the dataframe `right` to this one, reprojecting and merging tiles as necessary.
    * The operation is logically a "left outer" join, with the left side also determining the target CRS and extents.
    * Right side may have multiple Tile columns. This variant allows for the specific geospatial columns and join
    * expression to be specified.
    *
    * @param right right dataframe
    * @param leftExtent this (left) dataframe's Extent column
    * @param joinExpr join expression
    * @param leftCRS this (left) datafrasme's CRS column
    * @param rightExtent right dataframe's CRS extent
    * @param rightCRS right dataframe's CRS column
    * @param resampleMethod string indicating method to use for resampling.
    * @return joined dataframe
    */
  def rasterJoin(right: DataFrame, joinExpr: Column, leftExtent: Column, leftCRS: Column, rightExtent: Column, rightCRS: Column, resampleMethod: GTResampleMethod): DataFrame =
    RasterJoin(self, right, joinExpr, leftExtent, leftCRS, rightExtent, rightCRS, resampleMethod, None)


  /** Layout contents of RasterFrame to a layer. Assumes CRS and extent columns exist. */
  def toLayer(tlm: TileLayerMetadata[SpatialKey]): RasterFrameLayer = ReprojectToLayer(self, tlm)

  /** Coerces this DataFrame to a RasterFrameLayer after ensuring it has:
    *
    * <ol type="a">
    * <li>a space or space-time key column
    * <li>one or more tile columns
    * <li>tile layout metadata
    * <ol>
    *
    * If any of the above are violated, and [[IllegalArgumentException]] is thrown.
    *
    * @return validated RasterFrameLayer
    * @throws IllegalArgumentException when constraints are not met.
    */
  @throws[IllegalArgumentException]
  def asLayer: RasterFrameLayer = {
    val potentialRF = certifyLayer(self)

    require(
      potentialRF.findSpatialKeyField.nonEmpty,
      "A RasterFrameLayer requires a column identified as a spatial key"
    )

    require(potentialRF.tileColumns.nonEmpty, "A RasterFrameLayer requires at least one tile column")

    require(
      Try(potentialRF.tileLayerMetadata).isSuccess,
      "A RasterFrameLayer requires embedded TileLayerMetadata"
    )

    potentialRF
  }

  /**
   * Convert DataFrame already in a uniform gridding into a RasterFrameLayer
   *
   * @param spatialKey The column where the spatial key is stored
   * @param tlm Metadata describing layout under which tiles were created. Note: no checking is
   *            performed to ensure metadata, key-space, and tiles are coherent.
   * @throws IllegalArgumentException when constraints outlined in `asLayer` are not met.
   * @return Encoded RasterFrameLayer
   */
  @throws[IllegalArgumentException]
  private[rasterframes]
  def asLayer(spatialKey: Column, tlm: TileLayerMetadata[SpatialKey]): RasterFrameLayer =
    setSpatialColumnRole(spatialKey, tlm).asLayer

  /**
   * Convert DataFrame already in a uniform gridding into a RasterFrameLayer
   *
   * @param spatialKey The column where the spatial key is stored
   * @param temporalKey The column tagged under the temporal role
   * @param tlm Metadata describing layout under which tiles were created. Note: no checking is
   *            performed to ensure metadata, key-space, and tiles are coherent.
   * @throws IllegalArgumentException when constraints outlined in `asLayer` are not met.
   * @return Encoded RasterFrameLayer
   */
  @throws[IllegalArgumentException]
  private[rasterframes]
  def asLayer(spatialKey: Column, temporalKey: Column, tlm: TileLayerMetadata[SpaceTimeKey]): RasterFrameLayer =
    setSpatialColumnRole(spatialKey, tlm)
      .setTemporalColumnRole(temporalKey)
      .asLayer

  /**
   * Converts [[DataFrame]] to a RasterFrameLayer if the following constraints are fulfilled:
   *
   * <ol type="a">
   * <li>a space or space-time key column
   * <li>one or more tile columns
   * <li>tile layout metadata
   * <ol>
   *
   * @return Some[RasterFrameLayer] if constraints fulfilled, [[None]] otherwise.
   */
  def asLayerSafely: Option[RasterFrameLayer] = Try(asLayer).toOption

  /**
   * Tests for the following conditions on the [[DataFrame]]:
   *
   * <ol type="a">
   * <li>a space or space-time key column
   * <li>one or more tile columns
   * <li>tile layout metadata
   * <ol>
   *
   * @return true if all constraints are fulfilled, false otherwise.
   */
  def isAlreadyLayer: Boolean = Try(asLayer).isSuccess

  /** Internal method for slapping the RasterFreameLayer seal of approval on a DataFrame.
   * Only call if if you are sure it has a spatial key and tile columns and TileLayerMetadata. */
  private[rasterframes] def certify = certifyLayer(self)
}
