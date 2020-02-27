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

package org.locationtech.rasterframes

import geotrellis.raster._
import geotrellis.raster.testkit.RasterMatchers
import org.apache.spark.sql.functions._
import org.locationtech.rasterframes.tiles.ProjectedRasterTile

class RasterFunctionsSpec extends TestEnvironment with RasterMatchers {
  import TestData._
  import spark.implicits._

  describe("Misc raster functions") {
    it("should render ascii art") {
      val df = Seq[Tile](ProjectedRasterTile(TestData.l8Labels)).toDF("tile")
      val r1 = df.select(rf_render_ascii($"tile"))
      val r2 = df.selectExpr("rf_render_ascii(tile)").as[String]
      r1.first() should be(r2.first())
      checkDocs("rf_render_ascii")
    }

    it("should render cells as matrix") {
      val df = Seq(randDoubleNDTile).toDF("tile")
      val r1 = df.select(rf_render_matrix($"tile"))
      val r2 = df.selectExpr("rf_render_matrix(tile)").as[String]
      r1.first() should be(r2.first())
      checkDocs("rf_render_matrix")
    }

    it("should resample") {
      def lowRes = {
        def base = ArrayTile(Array(1, 2, 3, 4), 2, 2)

        ProjectedRasterTile(base.convert(ct), extent, crs)
      }

      def upsampled = {
        // format: off
        def base = ArrayTile(Array(
          1, 1, 2, 2,
          1, 1, 2, 2,
          3, 3, 4, 4,
          3, 3, 4, 4
        ), 4, 4)
        // format: on
        ProjectedRasterTile(base.convert(ct), extent, crs)
      }

      // a 4, 4 tile to upsample by shape
      def fourByFour = TestData.projectedRasterTile(4, 4, 0, extent, crs, ct)

      def df = Seq(lowRes).toDF("tile")

      val maybeUp = df.select(rf_resample($"tile", lit(2))).as[ProjectedRasterTile].first()
      assertEqual(maybeUp, upsampled)

      def df2 = Seq((lowRes, fourByFour)).toDF("tile1", "tile2")

      val maybeUpShape = df2.select(rf_resample($"tile1", $"tile2")).as[ProjectedRasterTile].first()
      assertEqual(maybeUpShape, upsampled)

      // Downsample by double argument < 1
      def df3 = Seq(upsampled).toDF("tile").withColumn("factor", lit(0.5))

      assertEqual(df3.selectExpr("rf_resample(tile, 0.5)").as[ProjectedRasterTile].first(), lowRes)
      assertEqual(df3.selectExpr("rf_resample(tile, factor)").as[ProjectedRasterTile].first(), lowRes)

      checkDocs("rf_resample")
    }
  }
}
