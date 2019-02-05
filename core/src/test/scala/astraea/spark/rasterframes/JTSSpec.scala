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
 */

package astraea.spark.rasterframes

import com.vividsolutions.jts.geom._
import geotrellis.proj4.{LatLng, Sinusoidal, WebMercator}
import geotrellis.vector.{Point ⇒ GTPoint}

/**
 * Test rig for operations providing interop with JTS types.
 *
 * @since 12/16/17
 */
class JTSSpec extends TestEnvironment with TestData with StandardColumns {
  describe("JTS interop") {
    val rf = l8Sample(1).projectedRaster.toRF(10, 10).withBounds()
    it("should allow joining and filtering of tiles based on points") {
      import spark.implicits._

      val crs = rf.tileLayerMetadata.merge.crs
      val coords = Seq(
        "one" -> GTPoint(-78.6445222907, 38.3957546898).reproject(LatLng, crs).jtsGeom,
        "two" -> GTPoint(-78.6601240367, 38.3976614324).reproject(LatLng, crs).jtsGeom,
        "three" -> GTPoint( -78.6123381343, 38.4001666769).reproject(LatLng, crs).jtsGeom
      )

      val locs = coords.toDF("id", "point")
      withClue("join with point column") {
        assert(rf.join(locs, st_contains(BOUNDS_COLUMN, $"point")).count === coords.length)
        assert(rf.join(locs, st_intersects(BOUNDS_COLUMN, $"point")).count === coords.length)
      }

      withClue("point literal") {
        val point = coords.head._2
        assert(rf.filter(st_contains(BOUNDS_COLUMN, geomLit(point))).count === 1)
        assert(rf.filter(st_intersects(BOUNDS_COLUMN, geomLit(point))).count === 1)
        assert(rf.filter(BOUNDS_COLUMN intersects point).count === 1)
        assert(rf.filter(BOUNDS_COLUMN intersects GTPoint(point)).count === 1)
        assert(rf.filter(BOUNDS_COLUMN containsGeom point).count === 1)
      }

      withClue("exercise predicates") {
        val point = geomLit(coords.head._2)
        assert(rf.filter(st_covers(BOUNDS_COLUMN, point)).count === 1)
        assert(rf.filter(st_crosses(BOUNDS_COLUMN, point)).count === 0)
        assert(rf.filter(st_disjoint(BOUNDS_COLUMN, point)).count === rf.count - 1)
        assert(rf.filter(st_overlaps(BOUNDS_COLUMN, point)).count === 0)
        assert(rf.filter(st_touches(BOUNDS_COLUMN, point)).count === 0)
        assert(rf.filter(st_within(BOUNDS_COLUMN, point)).count === 0)
      }
    }

    it("should allow construction of geometry literals") {
      import JTS._
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
      val boxed = rf.select(BOUNDS_COLUMN, envelope(BOUNDS_COLUMN) as "env")
      assert(boxed.select($"env".as[Envelope]).first.getArea > 0)
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
        reproject_geometry($"ll", LatLng, WebMercator) as "wm2",
        reproject_geometry($"wm", WebMercator, LatLng) as "ll2",
        reproject_geometry(reproject_geometry($"ll", LatLng, Sinusoidal), Sinusoidal, WebMercator) as "wm3"
      ).as[(Geometry, Geometry, Geometry)]


      val (wm2, ll2, wm3) = rp.first()

      wm2 should matchGeom(webMercator, 0.00001)
      ll2 should matchGeom(latLng, 0.00001)
      wm3 should matchGeom(webMercator, 0.00001)


      df.createOrReplaceTempView("geom")

      val wm4 = sql("SELECT rf_reproject_geometry(ll, '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs', 'EPSG:3857') AS wm4 from geom")
        .as[Geometry].first()
      wm4 should matchGeom(webMercator, 0.00001)
    }
  }
}
