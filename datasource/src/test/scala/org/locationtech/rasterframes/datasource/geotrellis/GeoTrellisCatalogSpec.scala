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
package org.locationtech.rasterframes.datasource.geotrellis

import java.io.File

import org.locationtech.rasterframes._
import geotrellis.proj4.LatLng
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import org.apache.hadoop.fs.FileUtil
import org.locationtech.rasterframes.TestEnvironment
import org.scalatest.BeforeAndAfter

/**
 * @author echeipesh
 */
class GeoTrellisCatalogSpec
    extends TestEnvironment with TestData with BeforeAndAfter {

  lazy val testRdd = TestData.randomSpatioTemporalTileLayerRDD(10, 12, 5, 6)

  import sqlContext.implicits._

  before {
    val outputDir = new File(outputLocalPath)
    FileUtil.fullyDelete(outputDir)
    outputDir.deleteOnExit()
    lazy val writer = LayerWriter(outputDir.toURI)
    val index =  ZCurveKeyIndexMethod.byDay()
    writer.write(LayerId("layer-1", 0), testRdd, index)
    writer.write(LayerId("layer-2", 0), testRdd, index)
  }

  describe("Catalog reading") {
    it("should show two zoom levels") {
      val cat = sqlContext.read
        .geotrellisCatalog(outputLocal.toUri)
      assert(cat.schema.length > 4)
      assert(cat.count() === 2)
    }

    it("should support loading a layer in a nice way") {
      val cat = sqlContext.read
        .geotrellisCatalog(outputLocal.toUri)

      // Select two layers.
      val layer = cat
        .where($"crs" === LatLng.toProj4String)
        .select(geotrellis_layer)
        .collect
      assert(layer.length === 2)

      val lots = layer.map(sqlContext.read.geotrellis.loadRF).map(_.toDF).reduce(_ union _)
      assert(lots.count === 60)
    }
  }
}
