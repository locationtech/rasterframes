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

package org.locationtech.rasterframes.experimental.datasource.geojson
import org.apache.spark.sql.types.{LongType, MapType}
import org.locationtech.rasterframes.TestEnvironment

/**
 * Test rig for GeoJsonRelation.
 *
 * @since 5/2/18
 */
class GeoJsonDataSourceTest extends TestEnvironment {

  val example1 = getClass.getResource("/example.geojson").toURI.toASCIIString
  val example2 = getClass.getResource("/buildings.geojson").toURI.toASCIIString

  describe("GeoJson spark reader") {
    it("should read geometry without inference") {
      val results = spark.read
        .geojson
        .option(GeoJsonDataSource.INFER_SCHEMA, false)
        .load(example1)
      assert(results.columns.length === 2)
      assert(results.schema.fields(1).dataType.isInstanceOf[MapType])
      assert(results.count() === 3)
    }

    it("should read geometry with inference") {
      val results = spark.read
        .geojson
        .option(GeoJsonDataSource.INFER_SCHEMA, true)
        .load(example1)
      assert(results.columns.length === 4)
      assert(results.schema.fields(1).dataType == LongType)
      assert(results.count() === 3)
    }

    it("should handle file with 3D bbox") {
      val results = spark.read
        .geojson
        .option(GeoJsonDataSource.INFER_SCHEMA, true)
        .load(example2)

      results.show()
    }
  }

}
