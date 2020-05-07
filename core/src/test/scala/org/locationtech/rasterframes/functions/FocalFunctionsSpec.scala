/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2020 Astraea, Inc.
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

package org.locationtech.rasterframes.functions
import geotrellis.raster.testkit.RasterMatchers
import org.locationtech.rasterframes.TestEnvironment

class FocalFunctionsSpec extends TestEnvironment with RasterMatchers {
  describe("focal operations") {
    it("should provide focal mean") {
      checkDocs("rf_focal_mean")
      fail()
    }
    it("should provide focal mode") {
      checkDocs("rf_focal_mode")
      fail()
    }
    it("should provide focal median") {
      checkDocs("rf_focal_median")
      fail()
    }
    it("should provide focal min") {
      checkDocs("rf_focal_min")
      fail()
    }
    it("should provide focal max") {
      checkDocs("rf_focal_max")
      fail()
    }
    it("should provide focal Moran's I") {
      checkDocs("rf_focal_moransi")
      fail()
    }
    it("should provide convolve") {
      checkDocs("rf_convolve")
      fail()
    }
  }
}
