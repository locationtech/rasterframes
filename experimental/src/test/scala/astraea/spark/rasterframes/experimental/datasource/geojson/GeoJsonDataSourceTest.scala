/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea. Inc.
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
 *
 */

package astraea.spark.rasterframes.experimental.datasource.geojson

import astraea.spark.rasterframes.TestEnvironment
import org.apache.spark.sql.types.MapType

/**
 * Test rig for GeoJsonRelation.
 *
 * @since 5/2/18
 */
class GeoJsonDataSourceTest extends TestEnvironment {

  describe("GeoJson spark reader") {
    it("should read geometry") {
      val examplePath = getClass.getResource("/example.json").toURI.toASCIIString
      val results = spark.read.format("geojson").load(examplePath)
      assert(results.columns.length === 2)
      assert(results.schema.fields(1).dataType.isInstanceOf[MapType])
      assert(results.count() === 3)
    }
  }
}
