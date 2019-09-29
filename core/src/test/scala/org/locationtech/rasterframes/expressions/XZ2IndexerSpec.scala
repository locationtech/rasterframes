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
import org.locationtech.rasterframes._
import geotrellis.vector.Extent
import org.locationtech.rasterframes.TestEnvironment
import org.apache.spark.sql.functions.lit
import org.locationtech.rasterframes._
import encoders.serialized_literal
import geotrellis.raster.CellType
import org.apache.spark.sql.Encoders
import org.locationtech.geomesa.curve.XZ2SFC
import org.locationtech.rasterframes.ref.{InMemoryRasterSource, RasterSource}
import org.locationtech.rasterframes.tiles.ProjectedRasterTile
import org.scalatest.Inspectors

class XZ2IndexerSpec extends TestEnvironment with Inspectors {
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
  val sfc = XZ2SFC(18)
  val expected = testExtents.map(e => sfc.index(e.xmin, e.ymin, e.xmax, e.ymax))

  def reproject(dst: CRS)(e: Extent): Extent = e.reproject(LatLng, dst)

  describe("Spatial index generation") {
    import spark.implicits._
    it("should be SQL registered with docs") {
      checkDocs("rf_spatial_index")
    }
    it("should create index from Extent") {
      val crs: CRS = WebMercator
      val df = testExtents.map(reproject(crs)).map(Tuple1.apply).toDF("extent")
      val indexes = df.select(rf_spatial_index($"extent", serialized_literal(crs))).collect()

      forEvery(indexes.zip(expected)) { case (i, e) =>
        i should be (e)
      }
    }
    it("should create index from Geometry") {
      val crs: CRS = LatLng
      val df = testExtents.map(_.jtsGeom).map(Tuple1.apply).toDF("extent")
      val indexes = df.select(rf_spatial_index($"extent", serialized_literal(crs))).collect()

      forEvery(indexes.zip(expected)) { case (i, e) =>
        i should be (e)
      }
    }
    it("should create index from ProjectedRasterTile") {
      val crs: CRS = WebMercator
      val tile = TestData.randomTile(2, 2, CellType.fromName("uint8"))
      val prts = testExtents.map(reproject(crs)).map(ProjectedRasterTile(tile, _, crs))

      implicit val enc = Encoders.tuple(ProjectedRasterTile.prtEncoder, Encoders.scalaInt)
      // The `id` here is to deal with Spark auto projecting single columns dataframes and needing to provide an encoder
      val df = prts.zipWithIndex.toDF("proj_raster", "id")
      val indexes = df.select(rf_spatial_index($"proj_raster")).collect()

      forEvery(indexes.zip(expected)) { case (i, e) =>
        i should be (e)
      }
    }
    it("should create index from RasterSource") {
      val crs: CRS = WebMercator
      val tile = TestData.randomTile(2, 2, CellType.fromName("uint8"))
      val srcs = testExtents.map(reproject(crs)).map(InMemoryRasterSource(tile, _, crs): RasterSource).toDF("src")
      val indexes = srcs.select(rf_spatial_index($"src")).collect()

      forEvery(indexes.zip(expected)) { case (i, e) =>
        i should be (e)
      }

    }
    it("should work when CRS is LatLng") {

      val df = testExtents.map(Tuple1.apply).toDF("extent")
      val crs: CRS = LatLng
      val indexes = df.select(rf_spatial_index($"extent", serialized_literal(crs))).collect()

      forEvery(indexes.zip(expected)) { case (i, e) =>
        i should be (e)
      }
    }
  }
}
