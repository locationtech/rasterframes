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

package org.locationtech.rasterframes.expressions

import geotrellis.raster.Tile
import geotrellis.spark._
import geotrellis.spark.tiling.FloatingLayoutScheme
import geotrellis.vector.{Extent, ProjectedExtent}
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.encoders.serialized_literal
import org.locationtech.rasterframes.expressions.aggregates.ProjectedLayerMetadataAggregate
import org.locationtech.rasterframes.model.TileDimensions

class ProjectedLayerMetadataAggregateSpec extends TestEnvironment {

  import spark.implicits._

  describe("ProjectedLayerMetadataAggregate") {
    it("should collect metadata from RasterFrame") {
      val image = TestData.sampleGeoTiff
      val rf = image.projectedRaster.toLayer(60, 65)
      val crs = rf.crs

      val df = rf.withExtent()
        .select($"extent", $"tile").as[(Extent, Tile)]

      val tileDims = rf.tileLayerMetadata.merge.tileLayout.tileDimensions

      val (_, tlm) = df
        .map { case (ext, tile) => (ProjectedExtent(ext, crs), tile) }
        .rdd.collectMetadata[SpatialKey](FloatingLayoutScheme(tileDims._1, tileDims._2))

      val md = df.select(ProjectedLayerMetadataAggregate(crs, TileDimensions(tileDims), $"extent",
        serialized_literal(crs), rf_cell_type($"tile"), rf_dimensions($"tile")))
      val tlm2 = md.first()

      tlm2 should be(tlm)
    }
  }
}
