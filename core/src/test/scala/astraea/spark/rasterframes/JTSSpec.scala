/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Astraea, Inc.
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
 */

package astraea.spark.rasterframes

import astraea.spark.rasterframes.util._
import com.vividsolutions.jts.geom._
import geotrellis.proj4.LatLng
import geotrellis.vector.{Point => gtPoint}

/**
 * Test rig for operations providing interop with JTS types.
 *
 * @since 12/16/17
 */
class JTSSpec extends TestEnvironment with TestData with StandardColumns with IntelliJPresentationCompilerHack {
  import spark.implicits._

  describe("JTS interop") {
    val rf = l8Sample(1).projectedRaster.toRF(10, 10).withBounds()
    it("should allow joining and filtering of tiles based on points") {
      val crs = rf.tileLayerMetadata.widen.crs
      val coords = Seq(
        "one" -> gtPoint(-78.6445222907, 38.3957546898).reproject(LatLng, crs).jtsGeom,
        "two" -> gtPoint(-78.6601240367, 38.3976614324).reproject(LatLng, crs).jtsGeom,
        "three" -> gtPoint( -78.6123381343, 38.4001666769).reproject(LatLng, crs).jtsGeom
      )

      val locs = coords.toDF("id", "point")
      withClue("join with point column") {
        assert(rf.join(locs, st_contains(BOUNDS_COLUMN, $"point")).count === coords.length)
        assert(rf.join(locs, st_intersects(BOUNDS_COLUMN, $"point")).count === coords.length)
      }

      withClue("point literal") {
        val point = coords.head._2
        assert(rf.filter(st_contains(BOUNDS_COLUMN, geomLit(point))).count === 1)
        assert(rf.filter(st_intersects(BOUNDS_COLUMN, geomLit(point))).count === 1)
        assert(rf.filter(BOUNDS_COLUMN intersects point).count === 1)
        assert(rf.filter(BOUNDS_COLUMN intersects gtPoint(point)).count === 1)
        assert(rf.filter(BOUNDS_COLUMN containsGeom point).count === 1)
      }

      withClue("exercise predicates") {
        val point = geomLit(coords.head._2)
        assert(rf.filter(st_covers(BOUNDS_COLUMN, point)).count === 1)
        assert(rf.filter(st_crosses(BOUNDS_COLUMN, point)).count === 0)
        assert(rf.filter(st_disjoint(BOUNDS_COLUMN, point)).count === rf.count - 1)
        assert(rf.filter(st_overlaps(BOUNDS_COLUMN, point)).count === 0)
        assert(rf.filter(st_touches(BOUNDS_COLUMN, point)).count === 0)
        assert(rf.filter(st_within(BOUNDS_COLUMN, point)).count === 0)
      }
    }

    it("should allow construction of geometry literals") {
      import JTS._
      assert(dfBlank.select(geomLit(point)).first === point)
      assert(dfBlank.select(geomLit(line)).first === line)
      assert(dfBlank.select(geomLit(poly)).first === poly)
      assert(dfBlank.select(geomLit(mpoint)).first === mpoint)
      assert(dfBlank.select(geomLit(mline)).first === mline)
      assert(dfBlank.select(geomLit(mpoly)).first === mpoly)
      assert(dfBlank.select(geomLit(coll)).first === coll)
    }

    it("should provide a means of getting a bounding box") {
      val boxed = rf.select(BOUNDS_COLUMN, box2D(BOUNDS_COLUMN))
      assert(boxed.select($"box2d(bounds)".as[Envelope]).first.getArea > 0)
      assert(boxed.toDF("bounds", "bbox").select("bbox.*").schema.length === 4)
    }
  }
}
