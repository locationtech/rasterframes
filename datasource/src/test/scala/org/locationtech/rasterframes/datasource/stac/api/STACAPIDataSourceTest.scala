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

package org.locationtech.rasterframes.datasource.stac.api

import cats.syntax.option._
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegInt
import org.locationtech.rasterframes.TestEnvironment

class STACAPIDataSourceTest extends TestEnvironment {

  describe("STAC API spark reader") {
    it("Should read from Franklin service") {
      val results =
        spark
          .read
          .stacApi("https://franklin.nasa-hsi.azavea.com/", searchLimit = (30: NonNegInt).some)
          .load

      results.printSchema()

      results.rdd.partitions.length shouldBe 1
      results.count() shouldBe 30
    }
  }
}
