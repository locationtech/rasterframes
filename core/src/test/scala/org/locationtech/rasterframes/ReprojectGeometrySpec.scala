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

package org.locationtech.rasterframes

import geotrellis.proj4.{CRS, LatLng, Sinusoidal, WebMercator}
import org.apache.spark.sql.Encoders
import org.locationtech.jts.geom._
import org.scalatest.{FunSpec, Matchers}

/**
 * Test for geometry reprojection.
 *
 * @since 11/29/18
 */
class ReprojectGeometrySpec extends FunSpec
  with TestEnvironment with Matchers {
  import spark.implicits._

  describe("Geometry reprojection") {
    it("should allow reprojection geometry") {
      // Note: Test data copied from ReprojectSpec in GeoTrellis
      val fact = new GeometryFactory()
      val latLng: Geometry = fact.createLineString(Array(
        new Coordinate(-111.09374999999999, 34.784483415461345),
        new Coordinate(-111.09374999999999, 43.29919735147067),
        new Coordinate(-75.322265625, 43.29919735147067),
        new Coordinate(-75.322265625, 34.784483415461345),
        new Coordinate(-111.09374999999999, 34.784483415461345)
      ))

      val webMercator: Geometry = fact.createLineString(Array(
        new Coordinate(-12366899.680315234, 4134631.734001753),
        new Coordinate(-12366899.680315234, 5357624.186564572),
        new Coordinate(-8384836.254770693, 5357624.186564572),
        new Coordinate(-8384836.254770693, 4134631.734001753),
        new Coordinate(-12366899.680315234, 4134631.734001753)
      ))

      withClue("both literal crs") {

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
      }

      withClue("one literal crs") {
        implicit val enc = Encoders.tuple(jtsGeometryEncoder, jtsGeometryEncoder, crsEncoder)

        val df = Seq((latLng, webMercator, LatLng: CRS)).toDF("ll", "wm", "llCRS")

        val rp = df.select(
          st_reproject($"ll", $"llCRS", WebMercator) as "wm2",
          st_reproject($"wm", WebMercator, $"llCRS") as "ll2",
          st_reproject(st_reproject($"ll", $"llCRS", Sinusoidal), Sinusoidal, WebMercator) as "wm3"
        ).as[(Geometry, Geometry, Geometry)]


        val (wm2, ll2, wm3) = rp.first()

        wm2 should matchGeom(webMercator, 0.00001)
        ll2 should matchGeom(latLng, 0.00001)
        wm3 should matchGeom(webMercator, 0.00001)

      }
    }
  }

}
