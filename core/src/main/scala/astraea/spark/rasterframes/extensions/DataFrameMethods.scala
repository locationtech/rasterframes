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

package astraea.spark.rasterframes.extensions

import astraea.spark.rasterframes
import astraea.spark.rasterframes.StandardColumns._
import astraea.spark.rasterframes.ref.{LayerSpace, RasterRef}
import astraea.spark.rasterframes.util._
import astraea.spark.rasterframes.{MetadataKeys, RasterFrame}
import geotrellis.raster.Tile
import geotrellis.spark.io._
import geotrellis.spark.{SpaceTimeKey, SpatialComponent, SpatialKey, TemporalKey, TileLayerMetadata}
import geotrellis.util.MethodExtensions
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.functions._
import org.apache.spark.sql.rf.{RasterRefUDT, TileUDT}
import org.apache.spark.sql.types.{MetadataBuilder, StructField}
import org.apache.spark.sql.{Column, DataFrame, TypedColumn}
import spray.json.JsonFormat

import scala.util.Try

/**
 * Extension methods over [[DataFrame]].
 *
 * @since 7/18/17
 */
trait DataFrameMethods[DF <: DataFrame] extends MethodExtensions[DF] with MetadataKeys {
  import Implicits.{WithDataFrameMethods, WithMetadataBuilderMethods, WithMetadataMethods, WithRasterFrameMethods}

  private def selector(column: Column) = (attr: Attribute) ⇒
    attr.name == column.columnName || attr.semanticEquals(column.expr)

  /** Map over the Attribute representation of Columns, modifying the one matching `column` with `op`. */
  private[astraea] def mapColumnAttribute(column: Column, op: Attribute ⇒  Attribute): DF = {
    val analyzed = self.queryExecution.analyzed.output
    val selects = selector(column)
    val attrs = analyzed.map { attr ⇒
      if(selects(attr)) op(attr) else attr
    }
    self.select(attrs.map(a ⇒ new Column(a)): _*).asInstanceOf[DF]
  }

  private[astraea] def addColumnMetadata(column: Column, op: MetadataBuilder ⇒ MetadataBuilder): DF = {
    mapColumnAttribute(column, attr ⇒ {
      val md = new MetadataBuilder().withMetadata(attr.metadata)
      attr.withMetadata(op(md).build)
    })
  }

  private[astraea] def fetchMetadataValue[D](column: Column, reader: (Attribute) ⇒ D): Option[D] = {
    val analyzed = self.queryExecution.analyzed.output
    analyzed.find(selector(column)).map(reader)
  }

  private[astraea]
  def setSpatialColumnRole[K: SpatialComponent: JsonFormat](
    column: Column, md: TileLayerMetadata[K]): DF =
    addColumnMetadata(column,
      _.attachContext(md.asColumnMetadata).tagSpatialKey
    )

  private[astraea]
  def setTemporalColumnRole(column: Column): DF =
    addColumnMetadata(column, _.tagTemporalKey)

  /** Get the role tag the column plays in the RasterFrame, if any. */
  private[astraea]
  def getColumnRole(column: Column): Option[String] =
    fetchMetadataValue(column, _.metadata.getString(SPATIAL_ROLE_KEY))

  /** Get the names of the columns that are of type `Tile` */
  def tileColumns: Seq[TypedColumn[Any, Tile]] =
    self.schema.fields
      .filter(_.dataType.typeName.equalsIgnoreCase(TileUDT.typeName))
      .map(f ⇒ col(f.name).as[Tile])

  /** Get the spatial column. */
  def spatialKeyColumn: Option[TypedColumn[Any, SpatialKey]] = {
    val key = findSpatialKeyField
    key
      .map(_.name)
      .map(col(_).as[SpatialKey])
  }

  /** Get the temporal column, if any. */
  def temporalKeyColumn: Option[TypedColumn[Any, TemporalKey]] = {
    val key = findTemporalKeyField
    key.map(_.name).map(col(_).as[TemporalKey])
  }

  /** Find the field tagged with the requested `role` */
  private[rasterframes] def findRoleField(role: String): Option[StructField] =
    self.schema.fields.find(
      f ⇒
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
    self.columns.foldLeft(self)((df, c) ⇒ df.withColumnRenamed(c, s"$prefix$c").asInstanceOf[DF])

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

    require(potentialRF.tileColumns.nonEmpty, "A RasterFrame requires at least one tile column")

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
    setSpatialColumnRole(spatialKey, tlm).asRF

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
    setSpatialColumnRole(spatialKey, tlm)
      .setTemporalColumnRole(temporalKey)
      .asRF

  @throws[IllegalArgumentException]
  def asRF(space: LayerSpace): RasterFrame = {
    require(tileColumns.isEmpty, "This method doesn't yet support existing tile columns")
    // We have two use cases to consider: This is already a rasterframe and we need to
    // reproject it. If we have RasterRefs then we reproject those
    val (refFields, otherFields) = self.schema.fields
        .partition(_.dataType.typeName.equalsIgnoreCase(RasterRefUDT.typeName))

    val refCols = refFields.map(f ⇒ self(f.name).as[RasterRef])
    val otherCols = otherFields.map(f ⇒ self(f.name))

    val layer = self.select(otherCols :+ rasterframes.projectIntoLayer(refCols, space): _*)

    val tlm = space.asTileLayerMetadata
    layer.setSpatialColumnRole(SPATIAL_KEY_COLUMN, tlm).asRF
  }

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
  def asRFSafely: Option[RasterFrame] = Try(asRF).toOption

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
  def isRF: Boolean = Try(asRF).isSuccess

  /** Internal method for slapping the RasterFreame seal of approval on a DataFrame.
   * Only call if if you are sure it has a spatial key and tile columns and TileLayerMetadata. */
  private[astraea] def certify = certifyRasterframe(self)
}
