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

package org.locationtech.rasterframes.expressions
import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.raster.CellType
import geotrellis.vector._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.jts.JTSTypes
import org.locationtech.geomesa.curve.{XZ2SFC, Z2SFC}
import org.locationtech.rasterframes.{TestEnvironment, _}
import org.locationtech.rasterframes.encoders.{cachedSerializer, serialized_literal}
import org.locationtech.rasterframes.ref.{InMemoryRasterSource, RFRasterSource}
import org.locationtech.rasterframes.tiles.ProjectedRasterTile
import org.scalatest.Inspectors

class SFCIndexerSpec extends TestEnvironment with Inspectors {
  val testExtents = Seq(
    Extent(10, 10, 12, 12),
    Extent(9.0, 9.0, 13.0, 13.0),
    Extent(-180.0, -90.0, 180.0, 90.0),
    Extent(0.0, 0.0, 180.0, 90.0),
    Extent(0.0, 0.0, 20.0, 20.0),
    Extent(11.0, 11.0, 13.0, 13.0),
    Extent(9.0, 9.0, 11.0, 11.0),
    Extent(10.5, 10.5, 11.5, 11.5),
    Extent(11.0, 11.0, 11.0, 11.0),
    Extent(-180.0, -90.0, 8.0, 8.0),
    Extent(0.0, 0.0, 8.0, 8.0),
    Extent(9.0, 9.0, 9.5, 9.5),
    Extent(20.0, 20.0, 180.0, 90.0)
  )
  def reproject(dst: CRS)(e: Extent): Extent = e.reproject(LatLng, dst)

  val xzsfc = XZ2SFC(18)
  val zsfc = new Z2SFC(31)
  val xzExpected = testExtents.map(e => xzsfc.index(e.xmin, e.ymin, e.xmax, e.ymax))
  val zExpected = (crs: CRS) => testExtents.map(reproject(crs)).map(e => {
    val p = e.center.reproject(crs, LatLng)
    zsfc.index(p.x, p.y)
  })

  describe("Centroid extraction") {
    import org.locationtech.rasterframes.encoders.CatalystSerializer._
    val expected = testExtents.map(_.center)
    it("should extract from Extent") {
      val dt = schemaOf[Extent]
      val extractor = DynamicExtractors.centroidExtractor(dt)
      val inputs = testExtents.map(_.toInternalRow).map(extractor)
      forEvery(inputs.zip(expected)) { case (i, e) =>
        i should be(e)
      }
    }
    it("should extract from Geometry") {
      val dt = JTSTypes.GeometryTypeInstance
      val extractor = DynamicExtractors.centroidExtractor(dt)
      val inputs = testExtents.map(_.toPolygon()).map(dt.serialize).map(extractor)
      forEvery(inputs.zip(expected)) { case (i, e) =>
        i should be(e)
      }
    }
    it("should extract from ProjectedRasterTile") {
      val crs: CRS = WebMercator
      val tile = TestData.randomTile(2, 2, CellType.fromName("uint8"))
      val dt = ProjectedRasterTile.prtEncoder.schema
      val extractor = DynamicExtractors.centroidExtractor(dt)
      val ser = cachedSerializer[ProjectedRasterTile]
      val inputs = testExtents
        .map(ProjectedRasterTile(tile, _, crs))
        .map(prt => ser(prt)).map(extractor)

      forEvery(inputs.zip(expected)) { case (i, e) =>
        i should be(e)
      }
    }
    it("should extract from RasterSource") {
      val crs: CRS = WebMercator
      val tile = TestData.randomTile(2, 2, CellType.fromName("uint8"))
      val dt = RasterSourceType
      val extractor = DynamicExtractors.centroidExtractor(dt)
      val inputs = testExtents.map(InMemoryRasterSource(tile, _, crs): RFRasterSource)
        .map(RasterSourceType.serialize).map(extractor)
      forEvery(inputs.zip(expected)) { case (i, e) =>
        i should be(e)
      }
    }
  }

  describe("Spatial index generation") {
    import spark.implicits._
    it("should be SQL registered with docs") {
      checkDocs("rf_xz2_index")
      checkDocs("rf_z2_index")
    }
    it("should create index from Extent") {
      val crs: CRS = WebMercator
      val df = testExtents.map(reproject(crs)).map(Tuple1.apply).toDF("extent")

      withClue("XZ2") {
        val indexes = df.select(rf_xz2_index($"extent", serialized_literal(crs))).collect()
        forEvery(indexes.zip(xzExpected)) { case (i, e) =>
          i should be(e)
        }
      }
      withClue("Z2") {
        val indexes = df.select(rf_z2_index($"extent", serialized_literal(crs))).collect()
        forEvery(indexes.zip(zExpected(crs))) { case (i, e) =>
          i should be(e)
        }
        indexes.distinct.length should be (indexes.length)
      }
    }
    it("should create index from Geometry") {
      val crs: CRS = LatLng
      val df = testExtents.map(_.toPolygon()).map(Tuple1.apply).toDF("extent")
      withClue("XZ2") {
        val indexes = df.select(rf_xz2_index($"extent", serialized_literal(crs))).collect()
        forEvery(indexes.zip(xzExpected)) { case (i, e) =>
          i should be(e)
        }
      }
      withClue("Z2") {
        val indexes = df.select(rf_z2_index($"extent", serialized_literal(crs))).collect()
        forEvery(indexes.zip(zExpected(crs))) { case (i, e) =>
          i should be(e)
        }
      }
    }
    it("should create index from ProjectedRasterTile") {
      val crs: CRS = WebMercator
      val tile = TestData.randomTile(2, 2, CellType.fromName("uint8"))
      val prts = testExtents.map(reproject(crs)).map(ProjectedRasterTile(tile, _, crs))

      implicit val enc = Encoders.tuple(ProjectedRasterTile.prtEncoder, Encoders.scalaInt)
      // The `id` here is to deal with Spark auto projecting single columns dataframes and needing to provide an encoder
      val df = prts.zipWithIndex.toDF("proj_raster", "id")
      withClue("XZ2") {
        val indexes = df.select(rf_xz2_index($"proj_raster")).collect()
        forEvery(indexes.zip(xzExpected)) { case (i, e) =>
          i should be(e)
        }
      }
      withClue("Z2") {
        val indexes = df.select(rf_z2_index($"proj_raster")).collect()
        forEvery(indexes.zip(zExpected(crs))) { case (i, e) =>
          i should be(e)
        }
      }
    }
    it("should create index from RasterSource") {
      val crs: CRS = WebMercator
      val tile = TestData.randomTile(2, 2, CellType.fromName("uint8"))
      val srcs = testExtents.map(reproject(crs)).map(InMemoryRasterSource(tile, _, crs): RFRasterSource).toDF("src")
      withClue("XZ2") {
        val indexes = srcs.select(rf_xz2_index($"src")).collect()
        forEvery(indexes.zip(xzExpected)) { case (i, e) =>
          i should be(e)
        }
      }
      withClue("Z2") {
        val indexes = srcs.select(rf_z2_index($"src")).collect()
        forEvery(indexes.zip(zExpected(crs))) { case (i, e) =>
          i should be(e)
        }
      }
    }
    it("should work when CRS is LatLng") {
      val df = testExtents.map(Tuple1.apply).toDF("extent")
      val crs: CRS = LatLng
      withClue("XZ2") {
        val indexes = df.select(rf_xz2_index($"extent", serialized_literal(crs))).collect()
        forEvery(indexes.zip(xzExpected)) { case (i, e) =>
          i should be(e)
        }
      }
      withClue("Z2") {
        val indexes = df.select(rf_z2_index($"extent", serialized_literal(crs))).collect()
        forEvery(indexes.zip(zExpected(crs))) { case (i, e) =>
          i should be(e)
        }
      }
    }
    it("should support custom resolution") {
      val df = testExtents.map(Tuple1.apply).toDF("extent")
      val crs: CRS = LatLng
      withClue("XZ2") {
        val sfc = XZ2SFC(3)
        val expected = testExtents.map(e => sfc.index(e.xmin, e.ymin, e.xmax, e.ymax, lenient = true))
        val indexes = df.select(rf_xz2_index($"extent", serialized_literal(crs), 3)).collect()
        forEvery(indexes.zip(expected)) { case (i, e) =>
          i should be(e)
        }
      }
      withClue("Z2") {
        val sfc = new Z2SFC(3)
        val expected = testExtents.map(e => sfc.index(e.center.x, e.center.y))
        val indexes = df.select(rf_z2_index($"extent", serialized_literal(crs), 3)).collect()
        forEvery(indexes.zip(expected)) { case (i, e) =>
          i should be(e)
        }
      }
    }
    it("should be lenient from RasterSource") {
      val extents = Seq(
        Extent(-181, -91, -179.5, -89.5),
        Extent(-181, 89.5, -179.5, 91),
        Extent(179.5, -91, 181, -89.5),
        Extent(179.5, 89.5, 181, 91)
      )

      val crs: CRS = LatLng
      val tile = TestData.randomTile(2, 2, CellType.fromName("uint8"))
      val srcs = extents
        .map(InMemoryRasterSource(tile, _, crs): RFRasterSource)
        .toDF("src")

      withClue("XZ2") {
        val expected = extents.map(e ⇒ xzsfc.index(e.xmin, e.ymin, e.xmax, e.ymax, lenient = true))
        val indexes = srcs.select(rf_xz2_index($"src")).collect()
        forEvery(indexes.zip(expected)) { case (i, e) =>
          i should be(e)
        }
      }
      withClue("Z2") {
        val expected = extents.map({ e ⇒
          val p = e.center
          zsfc.index(p.x, p.y, lenient = true)
        })
        val indexes = srcs.select(rf_z2_index($"src")).collect()
        forEvery(indexes.zip(expected)) { case (i, e) =>
          i should be(e)
        }
      }
    }
  }
}
