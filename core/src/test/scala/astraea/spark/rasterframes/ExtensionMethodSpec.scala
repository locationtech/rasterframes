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

package astraea.spark.rasterframes

/**
 * Tests miscellaneous extension methods.
 *
 * @since 3/20/18
 */
//noinspection ScalaUnusedSymbol
class ExtensionMethodSpec extends TestEnvironment with TestData {
  lazy val rf = sampleTileLayerRDD.toRF

  describe("DataFrame exention methods") {
    it("should maintain original type") {
      val df = rf.withPrefixedColumnNames("_foo_")
      "val rf2: RasterFrame = df" should compile
    }
    it("should provide tagged column access") {
      val df = rf.drop("tile")
      "val Some(col) = df.spatialKeyColumn" should compile
    }
  }
  describe("RasterFrame exention methods") {
    it("should provide spatial key column") {
      noException should be thrownBy {
        rf.spatialKeyColumn
      }
      "val Some(col) = rf.spatialKeyColumn" shouldNot compile
    }
  }
}
