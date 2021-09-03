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

package org.locationtech.rasterframes.encoders

import frameless.{Injection, RecordEncoderField, TypedEncoder}

import java.net.URI
import java.sql.Timestamp
import org.locationtech.rasterframes.stats.{CellHistogram, CellStatistics, LocalCellStatistics}
import org.locationtech.jts.geom.Envelope
import geotrellis.proj4.CRS
import geotrellis.raster.{CellSize, CellType, Dimensions, GridBounds, Raster, Tile, TileLayout}
import geotrellis.layer._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, Expression, GetStructField, If, IsNull, Literal}
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, NewInstance, StaticInvoke}
import org.apache.spark.sql.catalyst.util.QuantileSummaries
import org.apache.spark.sql.{FramelessInternals, rf}
import org.apache.spark.sql.rf.{RasterSourceUDT, TileUDT}
import org.apache.spark.sql.types.{DataType, Metadata, StructField, StructType}
import org.locationtech.geomesa.spark.jts.encoders.SpatialEncoders
import org.locationtech.rasterframes.model.{CellContext, LongExtent, TileContext, TileDataContext}
import org.locationtech.rasterframes.util.KryoSupport

import java.nio.ByteBuffer
import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe._

/**
 * TODO: move this overload to GeoTrellis, the reason is in the generic method invocation and Integral in implicits
 */
object DimensionsInt {
  def apply(cols: Int, rows: Int): Dimensions[Int] = new Dimensions(cols, rows)
}

object EnvelopeLocal {
  def apply(minx: Double, maxx: Double, miny: Double, maxy: Double): Envelope = new Envelope(minx, maxx, miny, miny)
}

/**
 * Implicit encoder definitions for RasterFrameLayer types.
 */
trait StandardEncoders extends SpatialEncoders {
  object PrimitiveEncoders extends SparkBasicEncoders
  def expressionEncoder[T: TypeTag]: ExpressionEncoder[T] = ExpressionEncoder()
  // implicit def layoutDefinitionEncoder: ExpressionEncoder[LayoutDefinition] = ExpressionEncoder()
  // implicit def stkBoundsEncoder: ExpressionEncoder[KeyBounds[SpaceTimeKey]] = ExpressionEncoder()
  // implicit def extentEncoder: ExpressionEncoder[Extent] = ExpressionEncoder()
  implicit def crsSparkEncoder: ExpressionEncoder[CRS] = ExpressionEncoder()
  implicit def projectedExtentEncoder: ExpressionEncoder[ProjectedExtent] = ExpressionEncoder()
  implicit def temporalProjectedExtentEncoder: ExpressionEncoder[TemporalProjectedExtent] = ExpressionEncoder()
  // implicit def cellSizeEncoder: ExpressionEncoder[CellSize] = ExpressionEncoder()
  implicit def uriEncoder: ExpressionEncoder[URI] = URIEncoder()
  // implicit def envelopeEncoder: ExpressionEncoder[Envelope] = EnvelopeEncoder()
  implicit def timestampEncoder: ExpressionEncoder[Timestamp] = ExpressionEncoder()
  implicit def strMapEncoder: ExpressionEncoder[Map[String, String]] = ExpressionEncoder()
  implicit def cellStatsEncoder: ExpressionEncoder[CellStatistics] = ExpressionEncoder()
  implicit def cellHistEncoder: ExpressionEncoder[CellHistogram] = ExpressionEncoder()
  implicit def localCellStatsEncoder: ExpressionEncoder[LocalCellStatistics] = ExpressionEncoder()
  // implicit def tilelayoutEncoder: ExpressionEncoder[TileLayout] = ExpressionEncoder()
  // implicit def cellContextEncoder: ExpressionEncoder[CellContext] = ExpressionEncoder()
  // implicit def tileContextEncoder: ExpressionEncoder[TileContext] = ExpressionEncoder()

  implicit def quantileSummariesInjection: Injection[QuantileSummaries, Array[Byte]] =
    Injection(KryoSupport.serialize(_).array(), array => KryoSupport.deserialize[QuantileSummaries](ByteBuffer.wrap(array)))

  implicit def uriInjection: Injection[URI, String] = Injection(_.toString, new URI(_))

  implicit def quantileSummariesTypedEncoder: TypedEncoder[QuantileSummaries] = TypedEncoder.usingInjection

  implicit def quantileSummariesEncoder: ExpressionEncoder[QuantileSummaries] = typedExpressionEncoder[QuantileSummaries]

  implicit def envelopeTypedEncoder: TypedEncoder[Envelope] = new TypedEncoder[Envelope] {
    val fields: List[RecordEncoderField] = List(
      RecordEncoderField(0, "minX", TypedEncoder[Double]),
      RecordEncoderField(1, "maxX", TypedEncoder[Double]),
      RecordEncoderField(2, "minY", TypedEncoder[Double]),
      RecordEncoderField(3, "maxY", TypedEncoder[Double])
    )

    def nullable: Boolean = true

    def jvmRepr: DataType = FramelessInternals.objectTypeFor[Envelope]

    def catalystRepr: DataType = {
      val structFields = fields.map { field =>
        StructField(
          name = field.name,
          dataType = field.encoder.catalystRepr,
          nullable = field.encoder.nullable,
          metadata = Metadata.empty
        )
      }

      StructType(structFields)
    }

    def fromCatalyst(path: Expression): Expression = {
      val newArgs = fields.map { field =>
        field.encoder.fromCatalyst( GetStructField(path, field.ordinal, Some(field.name)) )
      }
      // TODO: sounds like we should abstract this
      val newExpr = NewInstance(classTag.runtimeClass, newArgs, jvmRepr, propagateNull = true)
      // val newExpr = StaticInvoke(EnvelopeLocal.getClass, jvmRepr, "apply", newArgs, propagateNull = true, returnNullable = false)

      val nullExpr = Literal.create(null, jvmRepr)
      If(IsNull(path), nullExpr, newExpr)
    }

    def toCatalyst(path: Expression): Expression = {
      val nameExprs = fields.map { field =>
        Literal(field.name)
      }

      val valueExprs = fields.map { field =>
        val fieldPath = Invoke(path, s"get${field.name.capitalize}", field.encoder.jvmRepr, Nil)
        field.encoder.toCatalyst(fieldPath)
      }

      // the way exprs are encoded in CreateNamedStruct
      val exprs = nameExprs.zip(valueExprs).flatMap {
        case (nameExpr, valueExpr) => nameExpr :: valueExpr :: Nil
      }

      val createExpr = CreateNamedStruct(exprs)
      val nullExpr = Literal.create(null, createExpr.dataType)
      If(IsNull(path), nullExpr, createExpr)
    }
  }

  implicit def dimensionsTypedEncoder: TypedEncoder[Dimensions[Int]] = new TypedEncoder[Dimensions[Int]] {
    val fields: List[RecordEncoderField] = List(
      RecordEncoderField(0, "cols", TypedEncoder[Int]),
      RecordEncoderField(1, "rows", TypedEncoder[Int]))

    def nullable: Boolean = true

    def jvmRepr: DataType = FramelessInternals.objectTypeFor[Dimensions[Int]]

    def catalystRepr: DataType = {
      val structFields = fields.map { field =>
        StructField(
          name = field.name,
          dataType = field.encoder.catalystRepr,
          nullable = field.encoder.nullable,
          metadata = Metadata.empty
        )
      }

      StructType(structFields)
    }

    def fromCatalyst(path: Expression): Expression = {
      val newArgs = fields.map { field =>
        field.encoder.fromCatalyst( GetStructField(path, field.ordinal, Some(field.name)) )
      }
      // TODO: sounds like we should abstract this
      //val newExpr = NewInstance(classTag.runtimeClass, newArgs, jvmRepr, propagateNull = true)
      val newExpr = StaticInvoke(DimensionsInt.getClass, jvmRepr, "apply", newArgs, propagateNull = true, returnNullable = false)

      val nullExpr = Literal.create(null, jvmRepr)
      If(IsNull(path), nullExpr, newExpr)
    }

    def toCatalyst(path: Expression): Expression = {
      val nameExprs = fields.map { field =>
        Literal(field.name)
      }

      val valueExprs = fields.map { field =>
        val fieldPath = Invoke(path, field.name, field.encoder.jvmRepr, Nil)
        field.encoder.toCatalyst(fieldPath)
      }

      // the way exprs are encoded in CreateNamedStruct
      val exprs = nameExprs.zip(valueExprs).flatMap {
        case (nameExpr, valueExpr) => nameExpr :: valueExpr :: Nil
      }

      val createExpr = CreateNamedStruct(exprs)
      val nullExpr = Literal.create(null, createExpr.dataType)
      If(IsNull(path), nullExpr, createExpr)
    }
  }

  /**
   * @note
   * Frameless cannot derive encoder for GridBounds because it lacks constructor from (int, int, int int)
   * Defining Injection is not suitable because Injection is used in derivation of encoder fields but is not an encoder.
   * Additionally Injection to Tuple4[Int, Int, Int, Int] would not have correct fields.
   */
  implicit def gridBoundsEncoder: TypedEncoder[GridBounds[Int]] = new TypedEncoder[GridBounds[Int]]() {
    val fields: List[RecordEncoderField] = List(
      RecordEncoderField(0, "colMin", TypedEncoder[Int]),
      RecordEncoderField(1, "rowMin", TypedEncoder[Int]),
      RecordEncoderField(2, "colMax", TypedEncoder[Int]),
      RecordEncoderField(3, "rowMax", TypedEncoder[Int]))

    def nullable: Boolean = true

    def jvmRepr: DataType = FramelessInternals.objectTypeFor[GridBounds[Int]]

    def catalystRepr: DataType = {
      val structFields = fields.map { field =>
        StructField(
          name = field.name,
          dataType = field.encoder.catalystRepr,
          nullable = field.encoder.nullable,
          metadata = Metadata.empty
        )
      }

      StructType(structFields)
    }

    def fromCatalyst(path: Expression): Expression = {
      val newArgs = fields.map { field =>
        field.encoder.fromCatalyst( GetStructField(path, field.ordinal, Some(field.name)) )
      }
      // TODO: sounds like we should abstract this
      //val newExpr = NewInstance(classTag.runtimeClass, newArgs, jvmRepr, propagateNull = true)
      val newExpr = StaticInvoke(classTag.runtimeClass, jvmRepr, "apply", newArgs, propagateNull = true, returnNullable = false)

      val nullExpr = Literal.create(null, jvmRepr)
      If(IsNull(path), nullExpr, newExpr)
    }

    def toCatalyst(path: Expression): Expression = {
      val nameExprs = fields.map { field =>
        Literal(field.name)
      }

      val valueExprs = fields.map { field =>
        val fieldPath = Invoke(path, field.name, field.encoder.jvmRepr, Nil)
        field.encoder.toCatalyst(fieldPath)
      }

      // the way exprs are encoded in CreateNamedStruct
      val exprs = nameExprs.zip(valueExprs).flatMap {
        case (nameExpr, valueExpr) => nameExpr :: valueExpr :: Nil
      }

      val createExpr = CreateNamedStruct(exprs)
      val nullExpr = Literal.create(null, createExpr.dataType)
      If(IsNull(path), nullExpr, createExpr)
    }
  }

  // import org.locationtech.rasterframes.{CrsType}

  //implicit val crsUDT = new rf.CrsUDT()

  implicit def tileLayerMetadataTypedEncoder[K: TypedEncoder: ClassTag]: TypedEncoder[TileLayerMetadata[K]] = new TypedEncoder[TileLayerMetadata[K]] {
    val fields: List[RecordEncoderField] = List(
      RecordEncoderField(0, "cellType", cellTypeTypedEncoder),
      RecordEncoderField(1, "layout", TypedEncoder[LayoutDefinition]),
      RecordEncoderField(2, "extent", TypedEncoder[Extent]),
      RecordEncoderField(3, "crs", TypedEncoder[CRS]),
      RecordEncoderField(4, "bounds", TypedEncoder[KeyBounds[K]])
    )

    def nullable: Boolean = true

    def jvmRepr: DataType = FramelessInternals.objectTypeFor[TileLayerMetadata[K]]

    def catalystRepr: DataType = {
      val structFields = fields.map { field =>
        StructField(
          name = field.name,
          dataType = field.encoder.catalystRepr,
          nullable = field.encoder.nullable,
          metadata = Metadata.empty
        )
      }

      StructType(structFields)
    }

    def fromCatalyst(path: Expression): Expression = {
      val newArgs = fields.map { field =>
        field.encoder.fromCatalyst( GetStructField(path, field.ordinal, Some(field.name)) )
      }
      // TODO: sounds like we should abstract this
      // val newExpr = NewInstance(classTag.runtimeClass, newArgs, jvmRepr, propagateNull = true)
      val newExpr = StaticInvoke(classTag.runtimeClass, jvmRepr, "apply", newArgs, propagateNull = true, returnNullable = false)

      val nullExpr = Literal.create(null, jvmRepr)
      If(IsNull(path), nullExpr, newExpr)
    }

    def toCatalyst(path: Expression): Expression = {
      val nameExprs = fields.map { field =>
        Literal(field.name)
      }

      val valueExprs = fields.map { field =>
        val fieldPath = Invoke(path, field.name, field.encoder.jvmRepr, Nil)
        field.encoder.toCatalyst(fieldPath)
      }

      // the way exprs are encoded in CreateNamedStruct
      val exprs = nameExprs.zip(valueExprs).flatMap {
        case (nameExpr, valueExpr) => nameExpr :: valueExpr :: Nil
      }

      val createExpr = CreateNamedStruct(exprs)
      val nullExpr = Literal.create(null, createExpr.dataType)
      If(IsNull(path), nullExpr, createExpr)
    }
  }


  implicit val RasterSourceType = new RasterSourceUDT
  implicit val implTileType: FramelessInternals.UserDefinedType[Tile] = new TileUDT
  // implicit val BoundsUDT = new BoundsUDT

  implicit def envelopeEncoder: ExpressionEncoder[Envelope] = typedExpressionEncoder
  implicit def longExtentEncoder: ExpressionEncoder[LongExtent] = typedExpressionEncoder
  implicit def extentEncoder: ExpressionEncoder[Extent] = typedExpressionEncoder
  implicit def cellSizeEncoder: ExpressionEncoder[CellSize] = typedExpressionEncoder
  implicit def tileLayoutEncoder: ExpressionEncoder[TileLayout] = typedExpressionEncoder
  implicit def spatialKeyEncoder: ExpressionEncoder[SpatialKey] = typedExpressionEncoder
  implicit def temporalKeyEncoder: ExpressionEncoder[TemporalKey] = typedExpressionEncoder
  implicit def spaceTimeKeyEncoder: ExpressionEncoder[SpaceTimeKey] = typedExpressionEncoder
  implicit def keyBoundsEncoder[K: TypedEncoder]: ExpressionEncoder[KeyBounds[K]] = typedExpressionEncoder[KeyBounds[K]]
  implicit def boundsEncoder[K: TypedEncoder]: ExpressionEncoder[Bounds[K]] = keyBoundsEncoder[KeyBounds[K]].asInstanceOf[ExpressionEncoder[Bounds[K]]]
  implicit def cellTypeEncoder: ExpressionEncoder[CellType] = typedExpressionEncoder(cellTypeTypedEncoder)
  implicit def dimensionsEncoder: ExpressionEncoder[Dimensions[Int]] = typedExpressionEncoder
  implicit def layoutDefinitionEncoder: ExpressionEncoder[LayoutDefinition] = typedExpressionEncoder
  // implicit def tileLayerMetadataEncoder[K: TypeTag]: ExpressionEncoder[TileLayerMetadata[K]] = ExpressionEncoder()
  implicit def tileLayerMetadataEncoder[K: TypedEncoder: ClassTag]: ExpressionEncoder[TileLayerMetadata[K]] = typedExpressionEncoder[TileLayerMetadata[K]]
  implicit def tileContextEncoder: ExpressionEncoder[TileContext] = typedExpressionEncoder
  implicit def tileDataContextEncoder: ExpressionEncoder[TileDataContext] = typedExpressionEncoder
  implicit def cellContextEncoder: ExpressionEncoder[CellContext] = typedExpressionEncoder

  // null.asInstanceOf[FramelessInternals.UserDefinedType[Tile]]
  implicit def singlebandTileTypedEncoder: TypedEncoder[Tile] = TypedEncoder.usingUserDefinedType[Tile](implTileType, classTag[Tile])
  implicit def rasterTypedEncoder: TypedEncoder[Raster[Tile]] = TypedEncoder.usingDerivation

  implicit def singlebandTileEncoder: ExpressionEncoder[Tile] = typedExpressionEncoder
  implicit def optionalTileEncoder: ExpressionEncoder[Option[Tile]] = typedExpressionEncoder
  implicit def rasterEncoder: ExpressionEncoder[Raster[Tile]] = typedExpressionEncoder

  // was here ReprojectToLayer.scala
  // implicit def spatialKeyExtentCRS: ExpressionEncoder[(SpatialKey, Extent, CRS)] = typedExpressionEncoder[(SpatialKey, Extent, CRS)]

  // implicit def tileLayerMetadataSpatialEncoder: ExpressionEncoder[TileLayerMetadata[SpatialKey]] = typedExpressionEncoder[TileLayerMetadata[SpatialKey]]
}

object StandardEncoders extends StandardEncoders
