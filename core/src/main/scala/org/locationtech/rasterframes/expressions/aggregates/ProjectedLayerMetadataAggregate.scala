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
import org.locationtech.rasterframes.encoders.CatalystSerializer
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import geotrellis.proj4.{CRS, Transform}
import geotrellis.raster._
import geotrellis.raster.reproject.{Reproject, ReprojectRasterExtent}
import geotrellis.layer._
import geotrellis.vector.Extent
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{Column, Row, TypedColumn}

class ProjectedLayerMetadataAggregate(destCRS: CRS, destDims: Dimensions[Int]) extends UserDefinedAggregateFunction {
  import ProjectedLayerMetadataAggregate._

  override def inputSchema: StructType = CatalystSerializer[InputRecord].schema

  override def bufferSchema: StructType = CatalystSerializer[BufferRecord].schema

  override def dataType: DataType = CatalystSerializer[TileLayerMetadata[SpatialKey]].schema

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = ()

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)) {
      val in = input.to[InputRecord]

      if(buffer.isNullAt(0)) {
        in.toBufferRecord(destCRS).write(buffer)
      }
      else {
        val br = buffer.to[BufferRecord]
        br.merge(in.toBufferRecord(destCRS)).write(buffer)
      }
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    (buffer1.isNullAt(0), buffer2.isNullAt(0)) match {
      case (false, false) ⇒
        val left = buffer1.to[BufferRecord]
        val right = buffer2.to[BufferRecord]
        left.merge(right).write(buffer1)
      case (true, false) ⇒ buffer2.to[BufferRecord].write(buffer1)
      case _ ⇒ ()
    }
  }

  override def evaluate(buffer: Row): Any = {
    import org.locationtech.rasterframes.encoders.CatalystSerializer._
    val buf = buffer.to[BufferRecord]

    if (buf.isEmpty) {
      throw new IllegalArgumentException("Can not collect metadata from empty data frame.")
    }

    val re = RasterExtent(buf.extent, buf.cellSize)
    val layout = LayoutDefinition(re, destDims.cols, destDims.rows)

    val kb = KeyBounds(layout.mapTransform(buf.extent))
    TileLayerMetadata(buf.cellType, layout, buf.extent, destCRS, kb).toRow
  }
}

object ProjectedLayerMetadataAggregate {
  import org.locationtech.rasterframes.encoders.StandardEncoders._

  /** Primary user facing constructor */
  def apply(destCRS: CRS, extent: Column, crs: Column, cellType: Column,  tileSize: Column): TypedColumn[Any, TileLayerMetadata[SpatialKey]] =
  // Ordering must match InputRecord schema
    new ProjectedLayerMetadataAggregate(destCRS, Dimensions(NOMINAL_TILE_SIZE, NOMINAL_TILE_SIZE))(extent, crs, cellType, tileSize).as[TileLayerMetadata[SpatialKey]]

  def apply(destCRS: CRS, destDims: Dimensions[Int], extent: Column, crs: Column, cellType: Column,  tileSize: Column): TypedColumn[Any, TileLayerMetadata[SpatialKey]] =
  // Ordering must match InputRecord schema
    new ProjectedLayerMetadataAggregate(destCRS, destDims)(extent, crs, cellType, tileSize).as[TileLayerMetadata[SpatialKey]]

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
    implicit val serializer: CatalystSerializer[InputRecord] = new CatalystSerializer[InputRecord]{
      override val schema: StructType = StructType(Seq(
        StructField("extent", CatalystSerializer[Extent].schema, false),
        StructField("crs", CrsType, false),
        StructField("cellType", CatalystSerializer[CellType].schema, false),
        StructField("tileSize", CatalystSerializer[Dimensions[Int]].schema, false)
      ))

      override protected def to[R](t: InputRecord, io: CatalystIO[R]): R =
        throw new IllegalStateException("InputRecord is input only.")

      override protected def from[R](t: R, io: CatalystIO[R]): InputRecord = InputRecord(
        io.get[Extent](t, 0),
        ???,
        io.get[CellType](t, 2),
        io.get[Dimensions[Int]](t, 3)
      )
    }
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
      val encoded = this.toRow
      for(i <- 0 until encoded.size) {
        buffer(i) = encoded(i)
      }
    }

    def isEmpty: Boolean = extent == null || cellType == null || cellSize == null
  }

  private[expressions]
  object BufferRecord {
    implicit val serializer: CatalystSerializer[BufferRecord] = new CatalystSerializer[BufferRecord] {
      override val schema: StructType = StructType(Seq(
        StructField("extent", CatalystSerializer[Extent].schema, true),
        StructField("cellType", CatalystSerializer[CellType].schema, true),
        StructField("cellSize", CatalystSerializer[CellSize].schema, true)
      ))

      override protected def to[R](t: BufferRecord, io: CatalystIO[R]): R = io.create(
        io.to(t.extent),
        io.to(t.cellType),
        io.to(t.cellSize)
      )

      override protected def from[R](t: R, io: CatalystIO[R]): BufferRecord = BufferRecord(
        io.get[Extent](t, 0),
        io.get[CellType](t, 1),
        io.get[CellSize](t, 2)
      )
    }
  }
}