/*
 * Copyright 2017 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package astraea.spark.rasterframes

import geotrellis.spark.io._
import geotrellis.spark.{SpaceTimeKey, SpatialComponent, SpatialKey, TemporalKey, TileLayerMetadata}
import geotrellis.util.MethodExtensions
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.gt._
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.{Column, DataFrame}
import spray.json.JsonFormat

import scala.util.Try
/**
 * Extension methods over [[DataFrame]].
 *
 * @author sfitch
 * @since 7/18/17
 */
trait DataFrameMethods extends MethodExtensions[DataFrame] {

  private def selector(column: Column) = (attr: Attribute) ⇒
    attr.name == column.columnName || attr.semanticEquals(column.expr)

  /** Map over the Attribute representation of Columns, modifying the one matching `column` with `op`. */
  private[astraea] def mapColumnAttribute(column: Column, op: Attribute ⇒  Attribute): DataFrame = {
    val analyzed = self.queryExecution.analyzed.output
    val selects = selector(column)
    val attrs = analyzed.map { attr ⇒
      if(selects(attr)) op(attr) else attr
    }
    self.select(attrs.map(a ⇒ new Column(a)): _*)
  }

  private[astraea] def addColumnMetadata(column: Column, op: MetadataBuilder ⇒ MetadataBuilder): DataFrame = {
    mapColumnAttribute(column, attr ⇒ {
      val md = new MetadataBuilder().withMetadata(attr.metadata)
      attr.withMetadata(op(md).build)
    })
  }

  private[astraea] def fetchMetadataValue[D](column: Column, reader: (Attribute) ⇒ D): Option[D] = {
    val analyzed = self.queryExecution.analyzed.output
    analyzed.find(selector(column)).map(reader)
  }

  /** Set column role tag for subsequent interpretation. */
  private[astraea] def setColumnRole(column: Column, roleName: String): DataFrame =
    addColumnMetadata(column, _.putString(SPATIAL_ROLE_KEY, roleName))

  private[astraea] def setSpatialColumnRole[K: SpatialComponent: JsonFormat](column: Column, md: TileLayerMetadata[K]) =
    addColumnMetadata(self(SPATIAL_KEY_COLUMN),
      _.putMetadata(CONTEXT_METADATA_KEY, md.asColumnMetadata)
        .putString(SPATIAL_ROLE_KEY, classOf[SpatialKey].getSimpleName)
    )

  /** Get the role tag the column plays in the RasterFrame, if any. */
  private[astraea] def getColumnRole(column: Column): Option[String] =
    fetchMetadataValue(column, _.metadata.getString(SPATIAL_ROLE_KEY))

  /** Converts this DataFrame to a RasterFrame after ensuring it has:
   *
   * <ol type="a">
   * <li>a space or space-time key column
   * <li>one or more tile columns
   * <li>tile layout metadata
   * <ol>
   *
   * If any of the above are violated, and [[IllegalArgumentException]] is thrown.
   *
   * @return validated RasterFrame
   * @throws IllegalArgumentException when constraints are not met.
   */
  @throws[IllegalArgumentException]
  def asRF: RasterFrame = {
    val potentialRF = certifyRasterframe(self)

    require(
      potentialRF.findSpatialKeyField.nonEmpty,
      "A RasterFrame requires a column identified as a spatial key"
    )

    require(potentialRF.tileColumns.nonEmpty, "A RasterFrame requires at least one tile colulmn")

    require(
      Try(potentialRF.tileLayerMetadata).isSuccess,
      "A RasterFrame requires embedded TileLayerMetadata"
    )

    potentialRF
  }

  /**
   * Convert DataFrame into a RasterFrame
   *
   * @param spatialKey The column where the spatial key is stored
   * @param tlm Metadata describing layout under which tiles were created. Note: no checking is
   *            performed to ensure metadata, key-space, and tiles are coherent.
   * @throws IllegalArgumentException when constraints outlined in `asRF` are not met.
   * @return Encoded RasterFrame
   */
  @throws[IllegalArgumentException]
  def asRF(spatialKey: Column, tlm: TileLayerMetadata[SpatialKey]): RasterFrame =
    self.setSpatialColumnRole(spatialKey, tlm).asRF

  /**
   * Convert DataFrame into a RasterFrame
   *
   * @param spatialKey The column where the spatial key is stored
   * @param temporalKey The column tagged under the temporal role
   * @param tlm Metadata describing layout under which tiles were created. Note: no checking is
   *            performed to ensure metadata, key-space, and tiles are coherent.
   * @throws IllegalArgumentException when constraints outlined in `asRF` are not met.
   * @return Encoded RasterFrame
   */
  @throws[IllegalArgumentException]
  def asRF(spatialKey: Column, temporalKey: Column, tlm: TileLayerMetadata[SpaceTimeKey]): RasterFrame =
    self.setSpatialColumnRole(spatialKey, tlm)
      .setColumnRole(temporalKey, classOf[TemporalKey].getSimpleName)
      .asRF

  /**
   * Converts [[DataFrame]] to a RasterFrame if the following constraints are fulfilled:
   *
   * <ol type="a">
   * <li>a space or space-time key column
   * <li>one or more tile columns
   * <li>tile layout metadata
   * <ol>
   *
   * @return Some[RasterFrame] if constraints fulfilled, [[None]] otherwise.
   */
  def asRFSafely: Option[RasterFrame] = Try(self.asRF).toOption

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
  def isRF: Boolean = Try(self.asRF).isSuccess

  /** Internal method for slapping the RasterFreame seal of approval on a DataFrame.
   * Only call if if you are sure it has a spatial key and tile columns and TileLayerMetadata. */
  private[astraea] def certify = certifyRasterframe(self)
}
