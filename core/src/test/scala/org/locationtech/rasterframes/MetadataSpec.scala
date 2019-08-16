/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Astraea, Inc.
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

import org.apache.spark.sql.types.MetadataBuilder

/**
 * Test rig for column metadata management.
 *
 * @since 9/6/17
 */
class MetadataSpec extends TestEnvironment with TestData  {
  import spark.implicits._

  private val sampleMetadata = new MetadataBuilder().putBoolean("haz", true).putLong("baz", 42).build()

  describe("Metadata storage") {
    it("should serialize and attach metadata") {
      //val rf = sampleGeoTiff.projectedRaster.toLayer(128, 128)
      val df = spark.createDataset(Seq((1, "one"), (2, "two"), (3, "three"))).toDF("num", "str")
      val withmeta = df.mapColumnAttribute($"num", attr ⇒ {
        attr.withMetadata(sampleMetadata)
      })

      val meta2 = withmeta.fetchMetadataValue($"num", _.metadata)
      assert(Some(sampleMetadata) === meta2)
    }

    it("should handle post-join duplicate column names") {
      val df1 = spark.createDataset(Seq((1, "one"), (2, "two"), (3, "three"))).toDF("num", "str")
      val df2 = spark.createDataset(Seq((1, "a"), (2, "b"), (3, "c"))).toDF("num", "str")
      val joined = df1.as("a").join(df2.as("b"), "num")

      val withmeta = joined.mapColumnAttribute(df1("str"), attr ⇒ {
        attr.withMetadata(sampleMetadata)
      })

      val meta2 = withmeta.fetchMetadataValue($"str", _.metadata)

      assert(Some(sampleMetadata) === meta2)
    }
  }
}
