/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2021 Azavea, Inc.
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
import geotrellis.layer.{SpatialKey, TileLayerMetadata}
import geotrellis.proj4.LatLng
import geotrellis.raster
import geotrellis.raster.{CellType, Dimensions, NoNoData, Tile}
import geotrellis.vector.Extent
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.rf._
import org.apache.spark.sql.types.StringType
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.model.TileContext
import org.locationtech.rasterframes.ref.RasterRef.RasterRefTile
import org.locationtech.rasterframes.ref.{RFRasterSource, RasterRef}
import org.locationtech.rasterframes.tiles.{PrettyRaster, ShowableTile}
import org.scalatest.Inspectors

class PrettyRasterSpec extends TestEnvironment with TestData with Inspectors {
  import TestData.randomTile

  spark.version

  /**
   * GOAL: Can PrettyRaster replace ProjectedRasterTile?
   * - used as result of DataSources
   * - used as result of expressions
   * - used as part of DataFrame syntax
   */
  describe("PrettyRaster") {
    import spark.implicits._

    it("serialize PrettyRaster with Tile"){
      val data = PrettyRaster(TileContext(Extent(0,0,1,1), LatLng), one.toArrayTile())

      val df = List(data).toDF()
      df.show()
      df.printSchema()
      val fs = df.as[PrettyRaster]
      val out = fs.first()
      out shouldBe data
    }

    it("serialize PrettyRaster with RasterRefTile"){
      val src = RFRasterSource(remoteCOGSingleband1)
      val fullRaster = RasterRef(src, 0, None, None)
      val tile = RasterRefTile(fullRaster)

      val data = PrettyRaster(TileContext(Extent(0,0,1,1), LatLng), tile)

      val df = List(data).toDF()
      df.show()
      df.printSchema()
      val fs = df.as[PrettyRaster]
      val out = fs.first()
      out shouldBe data
      // This happens without invoking read on RasterRef, so the tile remains lazy
    }
  }
}
