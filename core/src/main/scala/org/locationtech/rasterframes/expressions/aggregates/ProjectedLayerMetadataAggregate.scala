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

package org.locationtech.rasterframes.expressions.aggregates

import org.locationtech.rasterframes._
import org.locationtech.rasterframes.encoders._
import geotrellis.proj4.{CRS, Transform}
import geotrellis.raster._
import geotrellis.raster.reproject.{Reproject, ReprojectRasterExtent}
import geotrellis.layer._
import geotrellis.vector.Extent
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Column, Row, TypedColumn}

class ProjectedLayerMetadataAggregate(destCRS: CRS, destDims: Dimensions[Int]) extends UserDefinedAggregateFunction {
  import ProjectedLayerMetadataAggregate._

  override def inputSchema: StructType = InputRecord.inputRecordEncoder.schema

  override def bufferSchema: StructType = BufferRecord.bufferRecordEncoder.schema

  override def dataType: DataType = tileLayerMetadataEncoder[SpatialKey].schema

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = ()

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)) {
      val in =
        InputRecord
          .inputRecordEncoder
          .resolveAndBind()
          .createDeserializer()(
            RowEncoder(InputRecord.inputRecordEncoder.schema)
              .createSerializer()(input)
          )

      if(buffer.isNullAt(0)) {
        in.toBufferRecord(destCRS).write(buffer)
      } else {
        val br =
          BufferRecord
            .bufferRecordEncoder
            .resolveAndBind()
            .createDeserializer()(
              RowEncoder(BufferRecord.bufferRecordEncoder.schema)
                .createSerializer()(buffer)
            )

        br.merge(in.toBufferRecord(destCRS)).write(buffer)
      }

    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    (buffer1.isNullAt(0), buffer2.isNullAt(0)) match {
      case (false, false) =>
        val left =
          BufferRecord
            .bufferRecordEncoder
            .resolveAndBind()
            .createDeserializer()(
              RowEncoder(BufferRecord.bufferRecordEncoder.schema)
                .createSerializer()(buffer1)
            )
        val right =
          BufferRecord
            .bufferRecordEncoder
            .resolveAndBind()
            .createDeserializer()(
              RowEncoder(BufferRecord.bufferRecordEncoder.schema)
                .createSerializer()(buffer2)
            )
        left.merge(right).write(buffer1)
      case (true, false) =>
        BufferRecord
          .bufferRecordEncoder
          .resolveAndBind()
          .createDeserializer()(
            RowEncoder(BufferRecord.bufferRecordEncoder.schema)
              .createSerializer()(buffer2)
          ).write(buffer1)
      case _ => ()
    }
  }

  override def evaluate(buffer: Row): Any = {
    val buf =
      BufferRecord
        .bufferRecordEncoder
        .resolveAndBind()
        .createDeserializer()(
          RowEncoder(BufferRecord.bufferRecordEncoder.schema)
            .createSerializer()(buffer)
        )

    if (buf.isEmpty) {
      throw new IllegalArgumentException("Can not collect metadata from empty data frame.")
    }

    val re = RasterExtent(buf.extent, buf.cellSize)
    val layout = LayoutDefinition(re, destDims.cols, destDims.rows)

    val kb = KeyBounds(layout.mapTransform(buf.extent))
    val md = TileLayerMetadata(buf.cellType, layout, buf.extent, destCRS, kb)

    RowEncoder(tileLayerMetadataEncoder[SpatialKey].schema)
      .resolveAndBind()
      .createDeserializer()(
        tileLayerMetadataEncoder[SpatialKey]
          .createSerializer()(md)
      )

  }
}

object ProjectedLayerMetadataAggregate {
  /** Primary user facing constructor */
  def apply(destCRS: CRS, extent: Column, crs: Column, cellType: Column,  tileSize: Column): TypedColumn[Any, TileLayerMetadata[SpatialKey]] =
  // Ordering must match InputRecord schema
    new ProjectedLayerMetadataAggregate(destCRS, Dimensions(NOMINAL_TILE_SIZE, NOMINAL_TILE_SIZE))(extent, crs, cellType, tileSize).as[TileLayerMetadata[SpatialKey]]

  def apply(destCRS: CRS, destDims: Dimensions[Int], extent: Column, crs: Column, cellType: Column,  tileSize: Column): TypedColumn[Any, TileLayerMetadata[SpatialKey]] = {
  // Ordering must match InputRecord schema
    new ProjectedLayerMetadataAggregate(destCRS, destDims)(extent, crs, cellType, tileSize).as[TileLayerMetadata[SpatialKey]]

  }

  private[expressions]
  case class InputRecord(extent: Extent, crs: CRS, cellType: CellType, tileSize: Dimensions[Int]) {
    def toBufferRecord(destCRS: CRS): BufferRecord = {
      val transform = Transform(crs, destCRS)

      val re = ReprojectRasterExtent(
        RasterExtent(extent, tileSize.cols, tileSize.rows),
        transform, Reproject.Options.DEFAULT
      )

      BufferRecord(
        re.extent,
        cellType,
        re.cellSize
      )
    }
  }

  private[expressions]
  object InputRecord {
    implicit def inputRecordEncoder: ExpressionEncoder[InputRecord] = typedExpressionEncoder[InputRecord]
  }

  private[expressions]
  case class BufferRecord(extent: Extent, cellType: CellType, cellSize: CellSize) {
    def merge(that: BufferRecord): BufferRecord = {
      val ext = this.extent.combine(that.extent)
      val ct = this.cellType.union(that.cellType)
      val cs = if (this.cellSize.resolution < that.cellSize.resolution) this.cellSize else that.cellSize
      BufferRecord(ext, ct, cs)
    }

    def write(buffer: MutableAggregationBuffer): Unit = {
      val encoded: Row =
        RowEncoder(BufferRecord.bufferRecordEncoder.schema)
          .resolveAndBind()
          .createDeserializer()(
            BufferRecord
              .bufferRecordEncoder
              .createSerializer()(this)
          )

      for(i <- 0 until encoded.size) {
        buffer(i) = encoded(i)
      }
    }

    def isEmpty: Boolean = extent == null || cellType == null || cellSize == null
  }

  private[expressions]
  object BufferRecord {
    implicit def bufferRecordEncoder: ExpressionEncoder[BufferRecord] = typedExpressionEncoder
  }
}