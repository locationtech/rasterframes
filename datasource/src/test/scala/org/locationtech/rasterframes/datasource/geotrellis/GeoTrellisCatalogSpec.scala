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
package org.locationtech.rasterframes.datasource.geotrellis

import org.locationtech.rasterframes._
import geotrellis.proj4.LatLng
import geotrellis.store._
import geotrellis.spark.store.LayerWriter
import geotrellis.store.index.ZCurveKeyIndexMethod
import org.apache.hadoop.fs.FileUtil
import org.locationtech.rasterframes.TestEnvironment
import org.scalatest.BeforeAndAfter

/**
 * @author echeipesh
 */
class GeoTrellisCatalogSpec
    extends TestEnvironment with TestData with BeforeAndAfter {

  lazy val testRdd = TestData.randomSpatioTemporalTileLayerRDD(10, 12, 5, 6)

  import spark.implicits._

  before {
    FileUtil.fullyDelete(scratchDir.toFile)
    lazy val writer = LayerWriter(scratchDir.toUri)
    val index =  ZCurveKeyIndexMethod.byDay()
    writer.write(LayerId("layer-1", 0), testRdd, index)
    writer.write(LayerId("layer-2", 0), testRdd, index)
  }

  describe("Catalog reading") {
    it("should show two zoom levels") {
      val cat = spark.read
        .geotrellisCatalog(scratchDir.toUri)
      assert(cat.schema.length > 4)
      assert(cat.count() === 2)
    }

    it("should support loading a layer in a nice way") {
      val cat = spark.read
        .geotrellisCatalog(scratchDir.toUri)

      // Select two layers.
      val layer = cat
        .where($"crs" === LatLng.toProj4String)
        .select(geotrellis_layer)
        .collect
      assert(layer.length === 2)

      val lots = layer.map(spark.read.geotrellis.loadLayer).map(_.toDF).reduce(_ union _)
      assert(lots.count === 60)
    }
  }
}
