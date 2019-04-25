/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea, Inc.
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

import java.nio.file.{Files, Paths}

import geotrellis.proj4.LatLng
import geotrellis.vector.io._
import geotrellis.vector.io.json.JsonFeatureCollection
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.JavaConversions._

/**
 *
 *
 * @since 5/30/18
 */
class GeometryOperationsSpec extends TestEnvironment with TestData {
  val geoJson = {
    val p = Paths.get(getClass.getResource("/L8-Labels-Elkton-VA.geojson").toURI)
    Files.readAllLines(p).mkString("\n")
  }

  describe("Geometery operations") {
    import spark.implicits._
    it("should rasterize geometry") {
      val rf = l8Sample(1).projectedRaster.toRF.withGeometry()

      val features = geoJson.parseGeoJson[JsonFeatureCollection].getAllPolygonFeatures[JsObject]()
      val df = features.map(f ⇒ (
        f.geom.reproject(LatLng, rf.crs).jtsGeom,
        f.data.fields("id").asInstanceOf[JsNumber].value.intValue()
      )).toDF("geom", "__fid__")

      val toRasterize = rf.crossJoin(df)

      val tlm = rf.tileLayerMetadata.merge

      val (cols, rows) = tlm.layout.tileLayout.tileDimensions

      val rasterized = toRasterize.withColumn("rasterized", rasterize($"geom", GEOMETRY_COLUMN, $"__fid__", cols, rows))

      assert(rasterized.count() === df.count() * rf.count())
      assert(rasterized.select(tile_dimensions($"rasterized")).distinct().count() === 1)
      val pixelCount = rasterized.select(agg_data_cells($"rasterized")).first()
      assert(pixelCount < cols * rows)


      toRasterize.createOrReplaceTempView("stuff")
      val viaSQL = sql(s"select rf_rasterize(geom, geometry, __fid__, $cols, $rows) as rasterized from stuff")
      assert(viaSQL.select(agg_data_cells($"rasterized")).first === pixelCount)

      //rasterized.select($"rasterized".as[Tile]).foreach(t ⇒ t.renderPng(ColorMaps.IGBP).write("target/" + t.hashCode() + ".png"))
    }
  }
}
