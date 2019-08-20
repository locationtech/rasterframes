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
  }
}
