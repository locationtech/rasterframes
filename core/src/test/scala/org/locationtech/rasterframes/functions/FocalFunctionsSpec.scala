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
import geotrellis.raster.Tile
import geotrellis.raster.mapalgebra.local.Implicits._
import org.locationtech.rasterframes.encoders.serialized_literal

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

    lazy val df = Seq(Option(ProjectedRasterTile(bufferedRaster, src.extent, src.crs))).toDF("proj_raster").cache()

    it("should perform focal mean") {
      checkDocs("rf_focal_mean")
      val actual =
        df
          .select(rf_focal_mean($"proj_raster", Square(1)))
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      val actualExpr =
        df
          .selectExpr(s"rf_focal_mean(proj_raster, 'square-1', 'all')")
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      assertEqual(actual, actualExpr)
      assertEqual(bt.focalMean(Square(1)), actual)
      assertEqual(fullTile.focalMean(Square(1)).crop(subGridBounds), actual)
    }
    it("should perform focal median") {
      checkDocs("rf_focal_median")
      val actual =
        df
          .select(rf_focal_median($"proj_raster", Square(1)))
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      val actualExpr =
        df
          .selectExpr(s"rf_focal_median(proj_raster, 'square-1', 'all')")
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      assertEqual(actual, actualExpr)
      assertEqual(bt.focalMedian(Square(1)), actual)
      assertEqual(fullTile.focalMedian(Square(1)).crop(subGridBounds), actual)
    }
    it("should perform focal mode") {
      checkDocs("rf_focal_mode")
      val actual =
        df
          .select(rf_focal_mode($"proj_raster", Square(1)))
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      val actualExpr =
        df
          .selectExpr(s"rf_focal_mode(proj_raster, 'square-1', 'all')")
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      assertEqual(actual, actualExpr)
      assertEqual(bt.focalMode(Square(1)), actual)
      assertEqual(fullTile.focalMode(Square(1)).crop(subGridBounds), actual)
    }
    it("should perform focal max") {
      checkDocs("rf_focal_max")
      val actual =
        df
          .select(rf_focal_max($"proj_raster", Square(1)))
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      val actualExpr =
        df
          .selectExpr(s"rf_focal_max(proj_raster, 'square-1', 'all')")
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      assertEqual(actual, actualExpr)
      assertEqual(bt.focalMax(Square(1)), actual)
      assertEqual(fullTile.focalMax(Square(1)).crop(subGridBounds), actual)
    }
    it("should perform focal min") {
      checkDocs("rf_focal_min")
      val actual =
        df
          .select(rf_focal_min($"proj_raster", Square(1)))
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      val actualExpr =
        df
          .selectExpr(s"rf_focal_min(proj_raster, 'square-1', 'all')")
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      assertEqual(actual, actualExpr)
      assertEqual(bt.focalMin(Square(1)), actual)
      assertEqual(fullTile.focalMin(Square(1)).crop(subGridBounds), actual)
    }
    it("should perform focal stddev") {
      checkDocs("rf_focal_moransi")
      val actual =
        df
          .select(rf_focal_stddev($"proj_raster", Square(1)))
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      val actualExpr =
        df
          .selectExpr(s"rf_focal_stddev(proj_raster, 'square-1', 'all')")
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      assertEqual(actual, actualExpr)
      assertEqual(bt.focalStandardDeviation(Square(1)), actual)
      assertEqual(fullTile.focalStandardDeviation(Square(1)).crop(subGridBounds), actual)
    }
    it("should perform focal Moran's I") {
      checkDocs("rf_focal_moransi")
      val actual =
        df
          .select(rf_focal_moransi($"proj_raster", Square(1)))
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      val actualExpr =
        df
          .selectExpr(s"rf_focal_moransi(proj_raster, 'square-1', 'all')")
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      assertEqual(actual, actualExpr)
      assertEqual(bt.tileMoransI(Square(1)), actual)
      assertEqual(fullTile.tileMoransI(Square(1)).crop(subGridBounds), actual)
    }
    it("should perform convolve") {
      checkDocs("rf_convolve")
      val actual =
        df
          .select(rf_convolve($"proj_raster", Kernel(Circle(2d))))
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      val actualExpr =
        df
          .withColumn("kernel", serialized_literal(Kernel(Circle(2d))))
          .selectExpr(s"rf_convolve(proj_raster, kernel, 'all')")
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      assertEqual(actual, actualExpr)
      assertEqual(bt.convolve(Kernel(Circle(2d))), actual)
      assertEqual(fullTile.convolve(Kernel(Circle(2d))).crop(subGridBounds), actual)
    }
    it("should perform slope") {
      checkDocs("rf_slope")
      val actual =
        df
          .select(rf_slope($"proj_raster", 1d))
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      val actualExpr =
        df
          .selectExpr(s"rf_slope(proj_raster, 1, 'all')")
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      assertEqual(actual, actualExpr)
      assertEqual(bt.slope(btCellSize, 1d), actual)
      assertEqual(fullTile.slope(btCellSize, 1d).crop(subGridBounds), actual)
    }
    it("should perform aspect") {
      checkDocs("rf_aspect")
      val actual =
        df
          .select(rf_aspect($"proj_raster"))
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      val actualExpr =
        df
          .selectExpr(s"rf_aspect(proj_raster, 'all')")
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      assertEqual(actual, actualExpr)
      assertEqual(bt.aspect(btCellSize), actual)
      assertEqual(fullTile.aspect(btCellSize).crop(subGridBounds), actual)
    }
    it("should perform hillshade") {
      checkDocs("rf_hillshade")
      val actual =
        df
          .select(rf_hillshade($"proj_raster", 315, 45, 1))
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      val actualExpr =
        df
          .selectExpr(s"rf_hillshade(proj_raster, 315, 45, 1, 'all')")
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      assertEqual(actual, actualExpr)
      assertEqual(bt.mapTile(_.hillshade(btCellSize, 315, 45, 1)), actual)
      assertEqual(fullTile.hillshade(btCellSize, 315, 45, 1).crop(subGridBounds), actual)
    }
    // that is the original use case
    // to read a buffered source, perform a focal operation
    // the followup functions would work with the buffered tile as
    // with a regular tile without a buffer (all ops will work within the window)
    it("should perform a focal operation and a valid local operation after that") {
      val actual =
        df
          .select(rf_aspect($"proj_raster").as("aspect"))
          .select(rf_local_add($"aspect", $"aspect"))
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      val a: Tile = bt.aspect(btCellSize)
      assertEqual(a.localAdd(a), actual)
    }

    // if we read a buffered tile the local buffer would preserve the buffer information
    // however rf_local_* functions don't preserve that type information
    // and the Buffer Tile is upcasted into the Tile and stored as a regular tile (within the buffer, with the buffer lost)
    // the follow up focal operation would be non buffered
    it("should perform a local operation and a valid focal operation after that with the buffer lost") {
      val actual =
        df
          .select(rf_local_add($"proj_raster", $"proj_raster") as "added")
          .select(rf_aspect($"added"))
          .as[Option[ProjectedRasterTile]]
          .first()
          .get
          .tile

      // that's what we would like eventually
      // val expected = bt.localAdd(bt) match {
        // case b: BufferTile => b.aspect(btCellSize)
        // case _             => throw new Exception("Not a Buffer Tile")
      // }

      // that's what we have actually
      // even though local ops can preserve the output tile
      // we don't handle that
      val expected = bt.localAdd(bt).aspect(btCellSize)
      assertEqual(expected, actual)
    }
  }
}
