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

package org.locationtech.rasterframes

import geotrellis.proj4.{LatLng, Sinusoidal, WebMercator}
import geotrellis.raster.Dimensions
import geotrellis.vector._
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import spray.json.JsNumber

/**
 * Test rig for operations providing interop with JTS types.
 *
 * @since 12/16/17
 */
class GeometryFunctionsSpec extends TestEnvironment with TestData with StandardColumns {
  import spark.implicits._

  describe("Vector geometry operations") {
    val rf = l8Sample(1).projectedRaster.toLayer(10, 10).withGeometry()
    it("should allow joining and filtering of tiles based on points") {
      import spark.implicits._

      val crs = rf.tileLayerMetadata.merge.crs
      val coords = Seq(
        "one" -> Point(-78.6445222907, 38.3957546898).reproject(LatLng, crs),
        "two" -> Point(-78.6601240367, 38.3976614324).reproject(LatLng, crs),
        "three" -> Point( -78.6123381343, 38.4001666769).reproject(LatLng, crs)
      )

      val locs = coords.toDF("id", "point")
      withClue("join with point column") {
        assert(rf.join(locs, st_contains(GEOMETRY_COLUMN, $"point")).count === coords.length)
        assert(rf.join(locs, st_intersects(GEOMETRY_COLUMN, $"point")).count === coords.length)
      }

      withClue("point literal") {
        val point = coords.head._2
        assert(rf.filter(st_contains(GEOMETRY_COLUMN, geomLit(point))).count === 1)
        assert(rf.filter(st_intersects(GEOMETRY_COLUMN, geomLit(point))).count === 1)
        assert(rf.filter(GEOMETRY_COLUMN intersects point).count === 1)
        assert(rf.filter(GEOMETRY_COLUMN intersects point).count === 1)
        assert(rf.filter(GEOMETRY_COLUMN containsGeom point).count === 1)
      }

      withClue("exercise predicates") {
        val point = geomLit(coords.head._2)
        assert(rf.filter(st_covers(GEOMETRY_COLUMN, point)).count === 1)
        assert(rf.filter(st_crosses(GEOMETRY_COLUMN, point)).count === 0)
        assert(rf.filter(st_disjoint(GEOMETRY_COLUMN, point)).count === rf.count - 1)
        assert(rf.filter(st_overlaps(GEOMETRY_COLUMN, point)).count === 0)
        assert(rf.filter(st_touches(GEOMETRY_COLUMN, point)).count === 0)
        assert(rf.filter(st_within(GEOMETRY_COLUMN, point)).count === 0)
      }
    }

    it("should allow construction of geometry literals") {
      import GeomData._
      assert(dfBlank.select(geomLit(point)).first === point)
      assert(dfBlank.select(geomLit(line)).first === line)
      assert(dfBlank.select(geomLit(poly)).first === poly)
      assert(dfBlank.select(geomLit(mpoint)).first === mpoint)
      assert(dfBlank.select(geomLit(mline)).first === mline)
      assert(dfBlank.select(geomLit(mpoly)).first === mpoly)
      assert(dfBlank.select(geomLit(coll)).first === coll)
    }

    it("should provide a means of getting a bounding box") {
      import spark.implicits._
      val boxed = rf.select(GEOMETRY_COLUMN, st_extent(GEOMETRY_COLUMN) as "extent")
      assert(boxed.select($"extent".as[Extent]).first.area > 0)
      assert(boxed.toDF("bounds", "bbox").select("bbox.*").schema.length === 4)
    }

    it("should allow reprojection geometry") {
      import spark.implicits._
      // Note: Test data copied from ReprojectSpec in GeoTrellis
      val fact = new GeometryFactory()
      val latLng: Geometry = fact.createLineString(Array(
        new Coordinate(-111.09374999999999,34.784483415461345),
        new Coordinate(-111.09374999999999,43.29919735147067),
        new Coordinate(-75.322265625,43.29919735147067),
        new Coordinate(-75.322265625,34.784483415461345),
        new Coordinate(-111.09374999999999,34.784483415461345)
      ))

      val webMercator: Geometry = fact.createLineString(Array(
        new Coordinate(-12366899.680315234,4134631.734001753),
        new Coordinate(-12366899.680315234,5357624.186564572),
        new Coordinate(-8384836.254770693,5357624.186564572),
        new Coordinate(-8384836.254770693,4134631.734001753),
        new Coordinate(-12366899.680315234,4134631.734001753)
      ))

      val df = Seq((latLng, webMercator)).toDF("ll", "wm")

      val rp = df.select(
        st_reproject($"ll", LatLng, WebMercator) as "wm2",
        st_reproject($"wm", WebMercator, LatLng) as "ll2",
        st_reproject(st_reproject($"ll", LatLng, Sinusoidal), Sinusoidal, WebMercator) as "wm3"
      ).as[(Geometry, Geometry, Geometry)]


      val (wm2, ll2, wm3) = rp.first()

      wm2 should matchGeom(webMercator, 0.00001)
      ll2 should matchGeom(latLng, 0.00001)
      wm3 should matchGeom(webMercator, 0.00001)


      df.createOrReplaceTempView("geom")

      val wm4 = sql("SELECT st_reproject(ll, '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs', 'EPSG:3857') AS wm4 from geom")
        .as[Geometry].first()
      wm4 should matchGeom(webMercator, 0.00001)
      checkDocs("st_reproject")
    }
  }

  it("should rasterize geometry") {
    val rf = l8Sample(1).projectedRaster.toLayer.withGeometry()
    val df = GeomData.features.map(f ⇒ (
      f.geom.reproject(LatLng, rf.crs),
      f.data("id").flatMap(_.asNumber).flatMap(_.toInt).getOrElse(0)
    )).toDF("geom", "__fid__")

    val toRasterize = rf.crossJoin(df)

    val tlm = rf.tileLayerMetadata.merge

    val Dimensions(cols, rows) = tlm.layout.tileLayout.tileDimensions

    val rasterized = toRasterize.withColumn("rasterized", rf_rasterize($"geom", GEOMETRY_COLUMN, $"__fid__", cols, rows))

    assert(rasterized.count() === df.count() * rf.count())
    assert(rasterized.select(rf_dimensions($"rasterized")).distinct().count() === 1)
    val pixelCount = rasterized.select(rf_agg_data_cells($"rasterized")).first()
    assert(pixelCount < cols * rows)


    toRasterize.createOrReplaceTempView("stuff")
    val viaSQL = sql(s"select rf_rasterize(geom, geometry, __fid__, $cols, $rows) as rasterized from stuff")
    assert(viaSQL.select(rf_agg_data_cells($"rasterized")).first === pixelCount)

    //rasterized.select($"rasterized".as[Tile]).foreach(t ⇒ t.renderPng(ColorMaps.IGBP).write("target/" + t.hashCode() + ".png"))
  }
}
