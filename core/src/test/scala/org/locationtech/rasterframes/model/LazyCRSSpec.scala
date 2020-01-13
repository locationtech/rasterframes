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

package org.locationtech.rasterframes.model

import geotrellis.proj4.{CRS, LatLng, Sinusoidal, WebMercator}
import org.scalatest._

class LazyCRSSpec extends FunSpec with Matchers {
  val sinPrj = "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs"
  val llPrj = "epsg:4326"


  describe("LazyCRS") {
    it("should implement equals") {
      LazyCRS(WebMercator) should be(LazyCRS(WebMercator))
      LazyCRS(WebMercator) should be(WebMercator)
      WebMercator should be(LazyCRS(WebMercator))
      LazyCRS(sinPrj) should be (Sinusoidal)
      CRS.fromString(sinPrj) should be (LazyCRS(Sinusoidal))
      LazyCRS(llPrj) should be(LatLng)
      LazyCRS(LatLng) should be(LatLng)
      LatLng should be(LazyCRS(llPrj))
      LatLng should be(LazyCRS(LatLng))
    }
    it("should interpret WKT1 GEOGCS correctly"){

      // This is from geotrellis.proj4.io.wkt.WKT.fromEpsgCode(4326)
      // Note it has subtle differences from other WKT1 forms
      val wktWGS84 = "GEOGCS[\"WGS 84\", DATUM[\"World Geodetic System 1984\", SPHEROID[\"WGS 84\", 6378137.0, 298.257223563, AUTHORITY[\"EPSG\",\"7030\"]], AUTHORITY[\"EPSG\",\"6326\"]], PRIMEM[\"Greenwich\", 0.0, AUTHORITY[\"EPSG\",\"8901\"]], UNIT[\"degree\", 0.017453292519943295], AXIS[\"Geodetic longitude\", EAST], AXIS[\"Geodetic latitude\", NORTH], AUTHORITY[\"EPSG\",\"4326\"]]"

      val crs = LazyCRS(wktWGS84)

      crs.toProj4String should startWith("+proj=longlat")
      crs.toProj4String should include("+datum=WGS84")
    }

    it("should interpret WKT1 PROJCS correctly") {

      // Via geotrellis.proj4.io.wkt.WKT.fromEpsgCode
      val wktUtm17N = "PROJCS[\"WGS 84 / UTM zone 17N\", GEOGCS[\"WGS 84\", DATUM[\"World Geodetic System 1984\", SPHEROID[\"WGS 84\", 6378137.0, 298.257223563, AUTHORITY[\"EPSG\",\"7030\"]], AUTHORITY[\"EPSG\",\"6326\"]], PRIMEM[\"Greenwich\", 0.0, AUTHORITY[\"EPSG\",\"8901\"]], UNIT[\"degree\", 0.017453292519943295], AXIS[\"Geodetic longitude\", EAST], AXIS[\"Geodetic latitude\", NORTH], AUTHORITY[\"EPSG\",\"4326\"]], PROJECTION[\"Transverse_Mercator\", AUTHORITY[\"EPSG\",\"9807\"]], PARAMETER[\"central_meridian\", -81.0], PARAMETER[\"latitude_of_origin\", 0.0], PARAMETER[\"scale_factor\", 0.9996], PARAMETER[\"false_easting\", 500000.0], PARAMETER[\"false_northing\", 0.0], UNIT[\"m\", 1.0], AXIS[\"Easting\", EAST], AXIS[\"Northing\", NORTH], AUTHORITY[\"EPSG\",\"32617\"]]"

      val utm17n = LazyCRS(wktUtm17N)
      utm17n.toProj4String should startWith("+proj=utm")
      utm17n.toProj4String should include("+zone=17")
      utm17n.toProj4String should include("+datum=WGS84")
    }

    ignore("should interpret WKT GEOCCS correctly"){
      // geotrellis.proj4.io.wkt. WKT.fromEpsgCode(4978) gives this but
      // .... fails on trying to instantiate
      val wktWgsGeoccs = "GEOCCS[\"WGS 84\", DATUM[\"World Geodetic System 1984\", SPHEROID[\"WGS 84\", 6378137.0, 298.257223563, AUTHORITY[\"EPSG\",\"7030\"]], AUTHORITY[\"EPSG\",\"6326\"]], PRIMEM[\"Greenwich\", 0.0, AUTHORITY[\"EPSG\",\"8901\"]], UNIT[\"m\", 1.0], AXIS[\"Geocentric X\", GEOCENTRIC_X], AXIS[\"Geocentric Y\", GEOCENTRIC_Y], AXIS[\"Geocentric Z\", GEOCENTRIC_Z], AUTHORITY[\"EPSG\",\"4978\"]]"

      val crs = LazyCRS(wktWgsGeoccs)
      crs.toProj4String should startWith("+proj=geocent")
      crs.toProj4String should include("+datum=WGS84")
    }
  }
}
