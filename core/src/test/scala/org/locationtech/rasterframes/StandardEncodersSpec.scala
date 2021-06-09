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
import geotrellis.layer.{KeyBounds, LayoutDefinition, SpatialKey, TileLayerMetadata}
import geotrellis.proj4.LatLng
import geotrellis.raster._
import geotrellis.vector._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StringType
import org.locationtech.rasterframes.model.{TileDataContext}
import org.locationtech.rasterframes.tiles.{PrettyRaster, ProjectedRasterTile}
import org.scalatest.Inspectors
/**
 * RasterFrameLayer test rig.
 *
 * @since 7/10/17
 */
class StandardEncodersSpec extends TestEnvironment with TestData with Inspectors {

  spark.version
  import spark.implicits._

  it("TileDataContext encoder") {
    val data = TileDataContext(IntCellType, Dimensions[Int](256, 256))
    val df = List(data).toDF()
    df.show()
    df.printSchema()
    val fs = df.as[TileDataContext]
    val out = fs.first()
    out shouldBe data
  }

  it("ProjectedExtent encoder") {
    val data = ProjectedExtent(Extent(0, 0, 1, 1), LatLng)
    val df = List(data).toDF()
    df.show()
    df.printSchema()
    df.select($"crs".cast(StringType)).show()
    val fs = df.as[ProjectedExtent]
    val out = fs.first()
    out shouldBe data
  }

  it("TileLayerMetadata encoder"){
    val data = TileLayerMetadata(
      IntCellType,
      LayoutDefinition(Extent(0,0,9,9), TileLayout(10, 10, 4, 4)),
      Extent(0,0,9,9),
      LatLng,
      KeyBounds(SpatialKey(0,0), SpatialKey(9,9)))

    val df = List(data).toDF()
    df.show()
    df.printSchema()
    val fs = df.as[TileLayerMetadata[SpatialKey]]
    val out = fs.first()
    out shouldBe data
  }

  it("ProjectedRasterTile encoder"){
    val enc = Encoders.product[PrettyRaster]
    print(enc.schema.treeString)
    print(ProjectedRasterTile.prtEncoder.schema.treeString)
  }

}
