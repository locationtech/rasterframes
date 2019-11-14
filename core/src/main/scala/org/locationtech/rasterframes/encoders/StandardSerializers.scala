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

package org.locationtech.rasterframes.encoders

import com.github.blemale.scaffeine.Scaffeine
import geotrellis.proj4.CRS
import geotrellis.raster._
import geotrellis.layer._
import geotrellis.vector._
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.Envelope
import org.locationtech.rasterframes.TileType
import org.locationtech.rasterframes.encoders.CatalystSerializer.{CatalystIO, _}
import org.locationtech.rasterframes.model.LazyCRS

/** Collection of CatalystSerializers for third-party types. */
trait StandardSerializers {

  implicit val envelopeSerializer: CatalystSerializer[Envelope] = new CatalystSerializer[Envelope] {
    override val schema: StructType = StructType(Seq(
      StructField("minX", DoubleType, false),
      StructField("maxX", DoubleType, false),
      StructField("minY", DoubleType, false),
      StructField("maxY", DoubleType, false)
    ))

    override protected def to[R](t: Envelope, io: CatalystIO[R]): R = io.create(
      t.getMinX, t.getMaxX, t.getMinY, t.getMaxX
    )

    override protected def from[R](t: R, io: CatalystIO[R]): Envelope = new Envelope(
      io.getDouble(t, 0), io.getDouble(t, 1), io.getDouble(t, 2), io.getDouble(t, 3)
    )
  }

  implicit val extentSerializer: CatalystSerializer[Extent] = new CatalystSerializer[Extent] {
    override val schema: StructType = StructType(Seq(
      StructField("xmin", DoubleType, false),
      StructField("ymin", DoubleType, false),
      StructField("xmax", DoubleType, false),
      StructField("ymax", DoubleType, false)
    ))

    override def to[R](t: Extent, io: CatalystIO[R]): R = io.create(
      t.xmin, t.ymin, t.xmax, t.ymax
    )

    override def from[R](row: R, io: CatalystIO[R]): Extent = Extent(
      io.getDouble(row, 0),
      io.getDouble(row, 1),
      io.getDouble(row, 2),
      io.getDouble(row, 3)
    )
  }

  implicit val gridBoundsSerializer: CatalystSerializer[GridBounds[Int]] = new CatalystSerializer[GridBounds[Int]] {
    override val schema: StructType = StructType(Seq(
      StructField("colMin", IntegerType, false),
      StructField("rowMin", IntegerType, false),
      StructField("colMax", IntegerType, false),
      StructField("rowMax", IntegerType, false)
    ))

    override protected def to[R](t: GridBounds[Int], io: CatalystIO[R]): R = io.create(
      t.colMin, t.rowMin, t.colMax, t.rowMax
    )

    override protected def from[R](t: R, io: CatalystIO[R]): GridBounds[Int] = GridBounds[Int](
      io.getInt(t, 0),
      io.getInt(t, 1),
      io.getInt(t, 2),
      io.getInt(t, 3)
    )
  }

  implicit val crsSerializer: CatalystSerializer[CRS] = new CatalystSerializer[CRS] {
    override val schema: StructType = StructType(Seq(
      StructField("crsProj4", StringType, false)
    ))

    override def to[R](t: CRS, io: CatalystIO[R]): R = io.create(
      io.encode(
        // Don't do this... it's 1000x slower to decode.
        //t.epsgCode.map(c => "EPSG:" + c).getOrElse(t.toProj4String)
        t.toProj4String
      )
    )

    override def from[R](row: R, io: CatalystIO[R]): CRS =
      LazyCRS(io.getString(row, 0))
  }

  implicit val cellTypeSerializer: CatalystSerializer[CellType] = new CatalystSerializer[CellType] {

    import StandardSerializers._

    override val schema: StructType = StructType(Seq(
      StructField("cellTypeName", StringType, false)
    ))

    override def to[R](t: CellType, io: CatalystIO[R]): R = io.create(
      io.encode(ct2sCache.get(t))
    )

    override def from[R](row: R, io: CatalystIO[R]): CellType =
      s2ctCache.get(io.getString(row, 0))
  }

  implicit val projectedExtentSerializer: CatalystSerializer[ProjectedExtent] = new CatalystSerializer[ProjectedExtent] {
    override val schema: StructType = StructType(Seq(
      StructField("extent", schemaOf[Extent], false),
      StructField("crs", schemaOf[CRS], false)
    ))

    override protected def to[R](t: ProjectedExtent, io: CatalystSerializer.CatalystIO[R]): R = io.create(
      io.to(t.extent),
      io.to(t.crs)
    )

    override protected def from[R](t: R, io: CatalystSerializer.CatalystIO[R]): ProjectedExtent = ProjectedExtent(
      io.get[Extent](t, 0),
      io.get[CRS](t, 1)
    )
  }

  implicit val spatialKeySerializer: CatalystSerializer[SpatialKey] = new CatalystSerializer[SpatialKey] {
    override val schema: StructType = StructType(Seq(
      StructField("col", IntegerType, false),
      StructField("row", IntegerType, false)
    ))

    override protected def to[R](t: SpatialKey, io: CatalystIO[R]): R = io.create(
      t.col,
      t.row
    )

    override protected def from[R](t: R, io: CatalystIO[R]): SpatialKey = SpatialKey(
      io.getInt(t, 0),
      io.getInt(t, 1)
    )
  }

  implicit val spacetimeKeySerializer: CatalystSerializer[SpaceTimeKey] = new CatalystSerializer[SpaceTimeKey] {
    override val schema: StructType = StructType(Seq(
      StructField("col", IntegerType, false),
      StructField("row", IntegerType, false),
      StructField("instant", LongType, false)
    ))

    override protected def to[R](t: SpaceTimeKey, io: CatalystIO[R]): R = io.create(
      t.col,
      t.row,
      t.instant
    )

    override protected def from[R](t: R, io: CatalystIO[R]): SpaceTimeKey = SpaceTimeKey(
      io.getInt(t, 0),
      io.getInt(t, 1),
      io.getLong(t, 2)
    )
  }

  implicit val cellSizeSerializer: CatalystSerializer[CellSize] = new CatalystSerializer[CellSize] {
    override val schema: StructType = StructType(Seq(
      StructField("width", DoubleType, false),
      StructField("height", DoubleType, false)
    ))

    override protected def to[R](t: CellSize, io: CatalystIO[R]): R = io.create(
      t.width,
      t.height
    )

    override protected def from[R](t: R, io: CatalystIO[R]): CellSize = CellSize(
      io.getDouble(t, 0),
      io.getDouble(t, 1)
    )
  }

  implicit val tileLayoutSerializer: CatalystSerializer[TileLayout] = new CatalystSerializer[TileLayout] {
    override val schema: StructType = StructType(Seq(
      StructField("layoutCols", IntegerType, false),
      StructField("layoutRows", IntegerType, false),
      StructField("tileCols", IntegerType, false),
      StructField("tileRows", IntegerType, false)
    ))

    override protected def to[R](t: TileLayout, io: CatalystIO[R]): R = io.create(
      t.layoutCols,
      t.layoutRows,
      t.tileCols,
      t.tileRows
    )

    override protected def from[R](t: R, io: CatalystIO[R]): TileLayout = TileLayout(
      io.getInt(t, 0),
      io.getInt(t, 1),
      io.getInt(t, 2),
      io.getInt(t, 3)
    )
  }

  implicit val layoutDefinitionSerializer = new CatalystSerializer[LayoutDefinition] {
    override val schema: StructType = StructType(Seq(
      StructField("extent", schemaOf[Extent], true),
      StructField("tileLayout", schemaOf[TileLayout], true)
    ))

    override protected def to[R](t: LayoutDefinition, io: CatalystIO[R]): R = io.create(
      io.to(t.extent),
      io.to(t.tileLayout)
    )

    override protected def from[R](t: R, io: CatalystIO[R]): LayoutDefinition = LayoutDefinition(
      io.get[Extent](t, 0),
      io.get[TileLayout](t, 1)
    )
  }

  implicit def boundsSerializer[T >: Null : CatalystSerializer]: CatalystSerializer[KeyBounds[T]] = new CatalystSerializer[KeyBounds[T]] {
    override val schema: StructType = StructType(Seq(
      StructField("minKey", schemaOf[T], true),
      StructField("maxKey", schemaOf[T], true)
    ))

    override protected def to[R](t: KeyBounds[T], io: CatalystIO[R]): R = io.create(
      io.to(t.get.minKey),
      io.to(t.get.maxKey)
    )

    override protected def from[R](t: R, io: CatalystIO[R]): KeyBounds[T] = KeyBounds(
      io.get[T](t, 0),
      io.get[T](t, 1)
    )
  }

  def tileLayerMetadataSerializer[T >: Null : CatalystSerializer]: CatalystSerializer[TileLayerMetadata[T]] = new CatalystSerializer[TileLayerMetadata[T]] {
    override val schema: StructType = StructType(Seq(
      StructField("cellType", schemaOf[CellType], false),
      StructField("layout", schemaOf[LayoutDefinition], false),
      StructField("extent", schemaOf[Extent], false),
      StructField("crs", schemaOf[CRS], false),
      StructField("bounds", schemaOf[KeyBounds[T]], false)
    ))

    override protected def to[R](t: TileLayerMetadata[T], io: CatalystIO[R]): R = io.create(
      io.to(t.cellType),
      io.to(t.layout),
      io.to(t.extent),
      io.to(t.crs),
      io.to(t.bounds.head)
    )

    override protected def from[R](t: R, io: CatalystIO[R]): TileLayerMetadata[T] = TileLayerMetadata(
      io.get[CellType](t, 0),
      io.get[LayoutDefinition](t, 1),
      io.get[Extent](t, 2),
      io.get[CRS](t, 3),
      io.get[KeyBounds[T]](t, 4)
    )
  }

  implicit def rasterSerializer: CatalystSerializer[Raster[Tile]] = new CatalystSerializer[Raster[Tile]] {

    import org.apache.spark.sql.rf.TileUDT.tileSerializer

    override val schema: StructType = StructType(Seq(
      StructField("tile", TileType, false),
      StructField("extent", schemaOf[Extent], false)
    ))

    override protected def to[R](t: Raster[Tile], io: CatalystIO[R]): R = io.create(
      io.to(t.tile),
      io.to(t.extent)
    )

    override protected def from[R](t: R, io: CatalystIO[R]): Raster[Tile] = Raster(
      io.get[Tile](t, 0),
      io.get[Extent](t, 1)
    )
  }

  implicit val spatialKeyTLMSerializer = tileLayerMetadataSerializer[SpatialKey]
  implicit val spaceTimeKeyTLMSerializer = tileLayerMetadataSerializer[SpaceTimeKey]

  implicit val tileDimensionsSerializer: CatalystSerializer[Dimensions[Int]] = new CatalystSerializer[Dimensions[Int]] {
    override val schema: StructType = StructType(Seq(
      StructField("cols", IntegerType, false),
      StructField("rows", IntegerType, false)
    ))

    override protected def to[R](t: Dimensions[Int], io: CatalystIO[R]): R = io.create(
      t.cols,
      t.rows
    )

    override protected def from[R](t: R, io: CatalystIO[R]): Dimensions[Int] = Dimensions[Int](
      io.getInt(t, 0),
      io.getInt(t, 1)
    )
  }
}

object StandardSerializers {
  private val s2ctCache = Scaffeine().build[String, CellType](
    (s: String) => CellType.fromName(s)
  )
  private val ct2sCache = Scaffeine().build[CellType, String](
    (ct: CellType) => ct.toString()
  )
}
