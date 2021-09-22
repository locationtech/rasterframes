/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2020 Astraea, Inc.
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

package org.locationtech.rasterframes.functions

import geotrellis.raster.mapalgebra.focal.{Circle, Kernel, Square}
import geotrellis.raster.{BufferTile, CellSize}
import geotrellis.raster.testkit.RasterMatchers
import org.locationtech.rasterframes.ref.{RFRasterSource, RasterRef, Subgrid}
import org.locationtech.rasterframes.tiles.ProjectedRasterTile
import org.locationtech.rasterframes._

import java.nio.file.Paths

class FocalFunctionsSpec extends TestEnvironment with RasterMatchers {

  import spark.implicits._

  describe("focal operations") {
    lazy val path =
      if(Paths.get("").toUri.toString.endsWith("core/")) Paths.get("src/test/resources/L8-B7-Elkton-VA.tiff").toUri
      else Paths.get("core/src/test/resources/L8-B7-Elkton-VA.tiff").toUri

    lazy val src = RFRasterSource(path)
    lazy val fullTile = src.read(src.extent).tile.band(0)

    // read a smaller region to read
    lazy val subGridBounds = src.gridBounds.buffer(-10)
    // read the region above, but buffered
    lazy val bufferedRaster = new RasterRef(src, 0, None, Some(Subgrid(subGridBounds)), 10)

    lazy val bt = BufferTile(fullTile, subGridBounds)
    lazy val btCellSize = CellSize(src.extent, bt.cols, bt.rows)

    it("should provide focal mean") {
      checkDocs("rf_focal_mean")
      val actual =
        Seq(Option(ProjectedRasterTile(bufferedRaster, src.extent, src.crs)))
          .toDF("proj_raster")
          .select(rf_focal_mean($"proj_raster", Square(1)))
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      assertEqual(bt.focalMean(Square(1)), actual)
      assertEqual(fullTile.focalMean(Square(1)).crop(subGridBounds), actual)
    }
    it("should provide focal median") {
      checkDocs("rf_focal_median")
      val actual =
        Seq(Option(ProjectedRasterTile(bufferedRaster, src.extent, src.crs)))
          .toDF("proj_raster")
          .select(rf_focal_median($"proj_raster", Square(1)))
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      assertEqual(bt.focalMedian(Square(1)), actual)
      assertEqual(fullTile.focalMedian(Square(1)).crop(subGridBounds), actual)
    }
    it("should provide focal mode") {
      checkDocs("rf_focal_mode")
      val actual =
        Seq(Option(ProjectedRasterTile(bufferedRaster, src.extent, src.crs)))
          .toDF("proj_raster")
          .select(rf_focal_mode($"proj_raster", Square(1)))
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      assertEqual(bt.focalMode(Square(1)), actual)
      assertEqual(fullTile.focalMode(Square(1)).crop(subGridBounds), actual)
    }
    it("should provide focal max") {
      checkDocs("rf_focal_max")
      val actual =
        Seq(Option(ProjectedRasterTile(bufferedRaster, src.extent, src.crs)))
          .toDF("proj_raster")
          .select(rf_focal_max($"proj_raster", Square(1)))
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      assertEqual(bt.focalMax(Square(1)), actual)
      assertEqual(fullTile.focalMax(Square(1)).crop(subGridBounds), actual)
    }
    it("should provide focal min") {
      checkDocs("rf_focal_min")
      val actual =
        Seq(Option(ProjectedRasterTile(bufferedRaster, src.extent, src.crs)))
          .toDF("proj_raster")
          .select(rf_focal_min($"proj_raster", Square(1)))
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      assertEqual(bt.focalMin(Square(1)), actual)
      assertEqual(fullTile.focalMin(Square(1)).crop(subGridBounds), actual)
    }
    it("should provide focal stddev") {
      checkDocs("rf_focal_moransi")
      val actual =
        Seq(Option(ProjectedRasterTile(bufferedRaster, src.extent, src.crs)))
          .toDF("proj_raster")
          .select(rf_focal_stddev($"proj_raster", Square(1)))
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      assertEqual(bt.focalStandardDeviation(Square(1)), actual)
      assertEqual(fullTile.focalStandardDeviation(Square(1)).crop(subGridBounds), actual)
    }
    it("should provide focal Moran's I") {
      checkDocs("rf_focal_moransi")
      val actual =
        Seq(Option(ProjectedRasterTile(bufferedRaster, src.extent, src.crs)))
          .toDF("proj_raster")
          .select(rf_focal_moransi($"proj_raster", Square(1)))
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      assertEqual(bt.tileMoransI(Square(1)), actual)
      assertEqual(fullTile.tileMoransI(Square(1)).crop(subGridBounds), actual)
    }
    it("should provide convolve") {
      checkDocs("rf_convolve")
      val actual =
        Seq(Option(ProjectedRasterTile(bufferedRaster, src.extent, src.crs)))
          .toDF("proj_raster")
          .select(rf_convolve($"proj_raster", Kernel(Circle(2d))))
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      assertEqual(bt.convolve(Kernel(Circle(2d))), actual)
      assertEqual(fullTile.convolve(Kernel(Circle(2d))).crop(subGridBounds), actual)
    }
    it("should provide slope") {
      checkDocs("rf_slope")
      val actual =
        Seq(Option(ProjectedRasterTile(bufferedRaster, src.extent, src.crs)))
          .toDF("proj_raster")
          .select(rf_slope($"proj_raster", 2d))
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      assertEqual(bt.slope(btCellSize, 2d), actual)
      assertEqual(fullTile.slope(btCellSize, 2d).crop(subGridBounds), actual)
    }
    it("should provide aspect") {
      checkDocs("rf_aspect")
      val actual =
        Seq(Option(ProjectedRasterTile(bufferedRaster, src.extent, src.crs)))
          .toDF("proj_raster")
          .select(rf_aspect($"proj_raster"))
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      assertEqual(bt.aspect(btCellSize), actual)
      assertEqual(fullTile.aspect(btCellSize).crop(subGridBounds), actual)
    }
    it("should provide hillshade") {
      checkDocs("rf_hillshade")
      val actual =
        Seq(Option(ProjectedRasterTile(bufferedRaster, src.extent, src.crs)))
          .toDF("proj_raster")
          .select(rf_hillshade($"proj_raster", 315, 45, 1))
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      assertEqual(bt.mapTile(_.hillshade(btCellSize, 315, 45, 1)), actual)
      assertEqual(fullTile.hillshade(btCellSize, 315, 45, 1).crop(subGridBounds), actual)
    }
  }
}
