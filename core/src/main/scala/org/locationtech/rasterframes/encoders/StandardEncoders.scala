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

import frameless.{RecordEncoderField, TypedEncoder}

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
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.FramelessInternals
import org.apache.spark.sql.rf.RasterSourceUDT
import org.apache.spark.sql.types.{DataType, Metadata, StructField, StructType}
import org.locationtech.geomesa.spark.jts.encoders.SpatialEncoders
import org.locationtech.rasterframes.model.{CellContext, TileContext, TileDataContext}

import scala.reflect.runtime.universe._

/**
 * Implicit encoder definitions for RasterFrameLayer types.
 */
trait StandardEncoders extends SpatialEncoders {
  object PrimitiveEncoders extends SparkBasicEncoders
  def expressionEncoder[T: TypeTag]: ExpressionEncoder[T] = ExpressionEncoder()
  implicit def spatialKeyEncoder: ExpressionEncoder[SpatialKey] = ExpressionEncoder()
  implicit def temporalKeyEncoder: ExpressionEncoder[TemporalKey] = ExpressionEncoder()
  implicit def spaceTimeKeyEncoder: ExpressionEncoder[SpaceTimeKey] = ExpressionEncoder()
  implicit def layoutDefinitionEncoder: ExpressionEncoder[LayoutDefinition] = ExpressionEncoder()
  implicit def stkBoundsEncoder: ExpressionEncoder[KeyBounds[SpaceTimeKey]] = ExpressionEncoder()
  implicit def extentEncoder: ExpressionEncoder[Extent] = ExpressionEncoder()
  implicit def singlebandTileEncoder: ExpressionEncoder[Tile] = ExpressionEncoder()
  implicit def rasterEncoder: ExpressionEncoder[Raster[Tile]] = ExpressionEncoder()
  implicit def tileLayerMetadataEncoder[K: TypeTag]: ExpressionEncoder[TileLayerMetadata[K]] = ExpressionEncoder()
  implicit def crsSparkEncoder: ExpressionEncoder[CRS] = ExpressionEncoder()
  implicit def projectedExtentEncoder: ExpressionEncoder[ProjectedExtent] = ExpressionEncoder()
  implicit def temporalProjectedExtentEncoder: ExpressionEncoder[TemporalProjectedExtent] = ExpressionEncoder()
  implicit def cellSizeEncoder: ExpressionEncoder[CellSize] = ExpressionEncoder()
  implicit def uriEncoder: ExpressionEncoder[URI] = URIEncoder()
  implicit def envelopeEncoder: ExpressionEncoder[Envelope] = EnvelopeEncoder()
  implicit def timestampEncoder: ExpressionEncoder[Timestamp] = ExpressionEncoder()
  implicit def strMapEncoder: ExpressionEncoder[Map[String, String]] = ExpressionEncoder()
  implicit def cellStatsEncoder: ExpressionEncoder[CellStatistics] = ExpressionEncoder()
  implicit def cellHistEncoder: ExpressionEncoder[CellHistogram] = ExpressionEncoder()
  implicit def localCellStatsEncoder: ExpressionEncoder[LocalCellStatistics] = ExpressionEncoder()
  implicit def tilelayoutEncoder: ExpressionEncoder[TileLayout] = ExpressionEncoder()
  implicit def cellContextEncoder: ExpressionEncoder[CellContext] = ExpressionEncoder()
  implicit def tileContextEncoder: ExpressionEncoder[TileContext] = ExpressionEncoder()
  implicit def tileDataContextEncoder: ExpressionEncoder[TileDataContext] = ExpressionEncoder()
  implicit def tileDimensionsEncoder: ExpressionEncoder[Dimensions[Int]] = ExpressionEncoder()

  implicit def cellTypeEncoder: ExpressionEncoder[CellType] = typedExpressionEncoder[CellType]

  /**
   * @note
   * Frameless cannot derive encoder for GridBounds because it lacks constructor from (int, int, int int)
   * Defining Injection is not suitable because Injection is used in derivation of encoder fields but is not an encoder.
   * Additionally Injection to Tuple4[Int, Int, Int, Int] would not have correct fields.
   */
  implicit def gridBoundsEncoder = new TypedEncoder[GridBounds[Int]]() {
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

  implicit val RasterSourceType = new RasterSourceUDT
}

object StandardEncoders extends StandardEncoders
