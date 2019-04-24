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

package org.locationtech.rasterframes.ml

import org.locationtech.rasterframes.TestData
import geotrellis.raster.Tile
import org.apache.spark.sql.functions.lit
import org.locationtech.rasterframes.TestEnvironment
/**
 *
 * @since 2/16/18
 */
class TileExploderSpec extends TestEnvironment with TestData {
  describe("Tile explode transformer") {
    it("should explode tiles") {
      import spark.implicits._
      val df = Seq[(Tile, Tile)]((byteArrayTile, byteArrayTile)).toDF("tile1", "tile2").withColumn("other", lit("stuff"))

      val exploder = new TileExploder()
      val newSchema = exploder.transformSchema(df.schema)

      val exploded = exploder.transform(df)
      assert(newSchema === exploded.schema)
      assert(exploded.columns.length === 5)
      assert(exploded.count() === 9)
      write(exploded)
    }
  }
}
