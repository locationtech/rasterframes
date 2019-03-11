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

package astraea.spark.rasterframes.encoders
import java.time.ZonedDateTime

import astraea.spark.rasterframes.encoders.StandardEncoders._
import astraea.spark.rasterframes.model.{CellContext, TileContext, TileDataContext, TileDimensions}
import astraea.spark.rasterframes.{TestData, TestEnvironment}
import geotrellis.proj4._
import geotrellis.raster.{CellSize, CellType, TileLayout, UShortUserDefinedNoDataCellType}
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.spark.{Bounds, KeyBounds, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.scalatest.Assertion

class CatalystSerializerSpec extends TestEnvironment with TestData {
  val dc = TileDataContext(UShortUserDefinedNoDataCellType(3), TileDimensions(12, 23))
  val tc = TileContext(Extent(1, 2, 3, 4), WebMercator)
  val cc = CellContext(tc, dc, 34, 45)
  val ext = Extent(1.2, 2.3, 3.4, 4.5)
  val tl = TileLayout(10, 10, 20, 20)
  val ct: CellType = UShortUserDefinedNoDataCellType(5.toShort)
  val ld = LayoutDefinition(ext, tl)
  val skb = KeyBounds[SpatialKey](SpatialKey(1, 2), SpatialKey(3, 4))


  def assertSerializerMatchesEncoder[T: CatalystSerializer: ExpressionEncoder](value: T): Assertion = {
    val enc = implicitly[ExpressionEncoder[T]]
    val ser = CatalystSerializer[T]
    ser.schema should be (enc.schema)
  }
  def assertConsistent[T: CatalystSerializer](value: T): Assertion = {
    val ser = CatalystSerializer[T]
    ser.toRow(value) should be(ser.toRow(value))
  }
  def assertInvertable[T: CatalystSerializer](value: T): Assertion = {
    val ser = CatalystSerializer[T]
    ser.fromRow(ser.toRow(value)) should be(value)
  }

  def assertContract[T: CatalystSerializer: ExpressionEncoder](value: T): Assertion = {
    assertConsistent(value)
    assertInvertable(value)
    assertSerializerMatchesEncoder(value)
  }

  describe("Specialized serialization on specific types") {
//    it("should support encoding") {
//      implicit val enc: ExpressionEncoder[CRS] = CatalystSerializerEncoder[CRS]()
//
//      //println(enc.deserializer.genCode(new CodegenContext))
//      val values = Seq[CRS](LatLng, Sinusoidal, ConusAlbers, WebMercator)
//      val df = spark.createDataset(values)(enc)
//      //df.show(false)
//      val results = df.collect()
//      results should contain allElementsOf values
//    }

    it("should serialize CRS") {
      val v: CRS = LatLng
      assertContract(v)
    }

    it("should serialize TileDataContext") {
      assertContract(dc)
    }

    it("should serialize TileContext") {
      assertContract(tc)
    }

    it("should serialize CellContext") {
      assertContract(cc)
    }

    it("should serialize ProjectedRasterTile") {
      val tile = TestData.projectedRasterTile(20, 30, -1.2, extent)
      assertContract(tile)
    }

    it("should serialize CellType") {
      assertContract(ct)
    }

    it("should serialize Extent") {
      assertContract(ext)
    }

    it("should eserialize ProjectedExtent") {
      val pe = ProjectedExtent(ext, ConusAlbers)
      assertContract(pe)
    }

    it("should eserialize SpatialKey") {
      val v = SpatialKey(2, 3)
      assertContract(v)
    }

    it("should eserialize SpaceTimeKey") {
      val v = SpaceTimeKey(2, 3, ZonedDateTime.now())
      assertContract(v)
    }

    it("should serialize CellSize") {
      val v = CellSize(extent, 50, 60)
      assertContract(v)
    }

    it("should serialize TileLayout") {
      assertContract(tl)
    }

    it("should serialize LayoutDefinition") {
      assertContract(ld)
    }

    it("should serialize Bounds[SpatialKey]") {
      implicit val skbEnc = ExpressionEncoder[KeyBounds[SpatialKey]]()
      assertContract(skb)
    }

    it("should serialize TileLayerMetata[SpatialKey]") {
      val tlm = TileLayerMetadata(ct, ld, ext, ConusAlbers, skb)
      assertContract(tlm)
    }
  }
}
