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

package astraea.spark.rasterframes
import astraea.spark.rasterframes.expressions.ExtractTile
import astraea.spark.rasterframes.tiles.ProjectedRasterTile
import geotrellis.proj4.LatLng
import geotrellis.raster.{ByteUserDefinedNoDataCellType, Tile}
import geotrellis.vector.Extent
import org.apache.spark.sql.Encoders
import org.scalatest.{FunSpec, Matchers}
import org.apache.spark.sql.functions._

class RasterFunctionsTest extends FunSpec
  with TestEnvironment with Matchers {
  import spark.implicits._

  val extent = Extent(10, 20, 30, 40)
  val crs = LatLng
  val ct = ByteUserDefinedNoDataCellType(-2)
  val cols = 10
  val rows = cols
  val one = TestData.projectedRasterTile(cols, rows, 1, extent, crs, ct)
  val two = TestData.projectedRasterTile(cols, rows, 2, extent, crs, ct)
  val three = TestData.projectedRasterTile(cols, rows, 3, extent, crs, ct)
  val nd = TestData.projectedRasterTile(cols, rows, -2, extent, crs, ct)

  implicit val pairEnc = Encoders.tuple(ProjectedRasterTile.prtEncoder, ProjectedRasterTile.prtEncoder)

  describe("binary tile operations") {
    it("should local_add") {
      val df = Seq((one, two)).toDF("one", "two")
      val maybeThree = df.select(local_add($"one", $"two")).as[ProjectedRasterTile]
      maybeThree.first() should be (three)
      df.selectExpr("rf_local_add(one, two)").as[ProjectedRasterTile].first() should be (three)

      val maybeThreeTile = df.select(local_add(ExtractTile($"one"), ExtractTile($"two"))).as[Tile]
      maybeThreeTile.show(false)
      maybeThreeTile.first() should be (three.toArrayTile())
    }
  }
}
