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

import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.{Raster, Tile}
import geotrellis.raster.render.ColorRamps
import org.locationtech.rasterframes.encoders.serialized_literal
import org.locationtech.rasterframes.expressions.aggregates.TileRasterizerAggregate
import org.apache.spark.sql.functions._
import org.locationtech.rasterframes.expressions.aggregates.TileRasterizerAggregate.ProjectedRasterDefinition


class RasterJoinSpec extends TestEnvironment with TestData {
  describe("Raster join between two DataFrames") {
    it("should join same scene in two projections, same tile size") {
      val s1 = readSingleband("L8-B4-Elkton-VA.tiff")
      val s2 = readSingleband("L8-B4-Elkton-VA-4326.tiff")

      val r1 = s1.projectedRaster.toRF(10, 10).withExtent().withColumn("crs", serialized_literal(s1.crs))
      val r2 = s2.projectedRaster.toRF(10, 10)
        .withExtent()
        .withColumn("crs", serialized_literal(s2.crs))
        .withColumnRenamed("tile", "tile2")

      val joined = r1.rasterJoin(r2)
      val result = joined.agg(TileRasterizerAggregate(
        ProjectedRasterDefinition(s1.cols, s1.rows, s1.cellType, s1.crs, s1.extent),
        col("crs"), col("extent"), col("tile2")) as "raster"
      ).select(col("raster").as[Raster[Tile]]).first()

      GeoTiff(result, s1.crs).write("target/out.tiff")
      result.extent shouldBe s1.extent
      //result.tile.renderPng(ColorRamps.greyscale(256)).write("target/out.png")
    }
  }

}
