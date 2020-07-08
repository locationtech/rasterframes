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

    it("should resample nearest") {
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

      val maybeUpDouble = df.select(rf_resample($"tile", 2.0)).as[ProjectedRasterTile].first()
      assertEqual(maybeUpDouble, upsampled)

      def df2 = Seq((lowRes, fourByFour)).toDF("tile1", "tile2")

      val maybeUpShape = df2.select(rf_resample($"tile1", $"tile2")).as[ProjectedRasterTile].first()
      assertEqual(maybeUpShape, upsampled)

      // Downsample by double argument < 1
      def df3 = Seq(upsampled).toDF("tile").withColumn("factor", lit(0.5))

      assertEqual(df3.selectExpr("rf_resample_nearest(tile, 0.5)").as[ProjectedRasterTile].first(), lowRes)
      assertEqual(df3.selectExpr("rf_resample_nearest(tile, factor)").as[ProjectedRasterTile].first(), lowRes)
      assertEqual(df3.selectExpr("rf_resample(tile, factor, \"nearest_neighbor\")").as[ProjectedRasterTile].first(), lowRes)

      checkDocs("rf_resample_nearest")
    }

    it("should resample aggregating") {
      checkDocs("rf_resample")

      // test of an aggregating method for  resample
      def original = {
        // format: off
        def base = ArrayTile(Array(
          1, 1, 2, 2,
          1, 3, 6, 2,
          3, 3, 4, 4,
          3, 7, 5, 4
        ), 4, 4)
        // format: on
        ProjectedRasterTile(base.convert(ct), extent, crs)
      }

      def expectedMax = ProjectedRasterTile(
        ArrayTile(Array(
          3, 6,
          7, 5), //2x2 tile
          2, 2).convert(ct), extent, crs)

      def expectedMode = ProjectedRasterTile(
        ArrayTile(Array(
          1, 2,
          3, 4
        ), 2, 2).convert(ct), extent, crs)

      def expectedAverage = ProjectedRasterTile(
        ArrayTile(Array(
          6.0/4, 12.0/4,
          4.0, 17.0/4),
          2, 2).convert(FloatConstantNoDataCellType), extent, crs)

      def df = Seq(original).toDF("tile")

      val maybeMax = df.select(rf_resample($"tile", 0.5, "Max")).as[ProjectedRasterTile].first()
      assertEqual(maybeMax, expectedMax)

      val maybeMode = df.select(rf_resample($"tile", 0.5, "mode")).as[ProjectedRasterTile].first()
      assertEqual(maybeMode, expectedMode)

      val maybeAverage = df.select(rf_resample($"tile", 0.5, "average")).as[ProjectedRasterTile].first()
      assertEqual(maybeAverage, expectedAverage)

    }
    it("should resample bilinear") {
      def original = {
        def base = ArrayTile(Array(
          0, 1, 2, 3,
          1, 2, 3, 4,
          2, 3, 4, 5,
          3, 4, 5, 6
        ), 4, 4)

        ProjectedRasterTile(base.convert(ct), extent, crs)
      }

      def expected2x2 = ProjectedRasterTile(
        ArrayTile(Array(
          1, 3,
          3, 5
        ), 2, 2).convert(FloatConstantNoDataCellType), extent, crs
      )

      def df = Seq(original).toDF("tile")
      val result = df.select(
        rf_resample($"tile", 0.5, "bilinear"))
        .as[ProjectedRasterTile].first()

      assertEqual(result, expected2x2)
    }

  }
}
