/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea, Inc.
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
package org.locationtech.rasterframes.datasource.geotiff

import java.io.{File, FilenameFilter}

import org.locationtech.rasterframes._
import org.locationtech.rasterframes.TestEnvironment

/**
 * @since 1/14/18
 */
class GeoTiffCollectionDataSourceSpec
    extends TestEnvironment with TestData {

  describe("GeoTiff directory reading") {
    it("shiould read a directory of files") {

      val df = spark.read
        .format("geotiff")
        .load(geotiffDir.resolve("*.tiff").toString)
      val expected = geotiffDir.toFile.list(new FilenameFilter {
        override def accept(dir: File, name: String): Boolean = name.endsWith("tiff")
      }).length

      assert(df.select("path").distinct().count() === expected)

      // df.show(false)
    }
  }
}
