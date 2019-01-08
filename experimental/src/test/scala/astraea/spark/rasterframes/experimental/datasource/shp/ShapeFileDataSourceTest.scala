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

package astraea.spark.rasterframes.experimental.datasource.shp
import astraea.spark.rasterframes.TestEnvironment
import org.apache.spark.sql.jts.AbstractGeometryUDT
/**
 *
 *
 * @since 2019-01-05
 */
class ShapeFileDataSourceTest extends TestEnvironment {
  describe("ShapeFile DataSource") {
    it("should read simple features from shapefile and sidecars") {
      val src = getClass.getResource("/louisaforest.shp").toExternalForm

      val df = spark.read.shapefile.load(src)

      assert(df.columns.length === 4)
      assert(df.schema.exists(_.dataType.isInstanceOf[AbstractGeometryUDT[_]]))

      //df.show(false)

      assert(df.count() === 1721)
      assert(df.select("cover_type").distinct().count() === 1)
    }

    it("should read simple features from shapefile archive") {
      val src = getClass.getResource("/louisaforest.zip").toExternalForm

      val df = spark.read.shapefile.load(src)

      assert(df.columns.length === 4)
      assert(df.schema.exists(_.dataType.isInstanceOf[AbstractGeometryUDT[_]]))

      //df.show(false)

      assert(df.count() === 1721)
      assert(df.select("cover_type").distinct().count() === 1)
    }

  }
}
