/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea. Inc.
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
 *
 */

package org.locationtech.rasterframes.experimental.datasource.awspds

import geotrellis.proj4.LatLng
import geotrellis.vector.Extent
import org.apache.spark.sql.functions._
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.datasource.raster._

/**
 * Test rig for L8 catalog stuff.
 *
 * @since 5/4/18
 */
class L8CatalogRelationTest extends TestEnvironment {
  import spark.implicits._

  val catalog = spark.read.l8Catalog.load()

  val scenes = catalog
    .where($"acquisition_date" === to_timestamp(lit("2017-04-04 15:12:55.394")))
    .where($"path" === 11 && $"row" === 12)
    .cache()

  describe("Representing L8 scenes as a Spark data source") {
    it("should provide a non-empty catalog") {
      scenes.count() shouldBe 1
    }

    it("should provide 11 band + 1 QA urls") {
      scenes.schema.count(_.name.startsWith("B")) shouldBe 12
    }

    it("should construct valid URLs") {
      val urlStr = scenes.select("B11").as[String].first
      val code = TestSupport.urlResponse(urlStr)
      code should be (200)
    }

    it("should work with SQL and spatial predicates") {
      catalog.createOrReplaceTempView("l8_catalog")
      val scenes = spark.sql("""
        SELECT st_geometry(bounds_wgs84) as geometry, acquisition_date, B1, B2
        FROM l8_catalog
        WHERE
         st_intersects(st_geometry(bounds_wgs84), st_geomFromText('LINESTRING (-39.551 -7.1881, -72.2461 -45.7062)')) AND
         acquisition_date > to_timestamp('2017-11-01') AND
         acquisition_date <= to_timestamp('2017-12-13')
        """)

      scenes.count() shouldBe > (100L)
    }

    it("should construct expected extents") {
      catalog.createOrReplaceTempView("l8_catalog")

      catalog.filter($"bounds_wgs84.xmin" > $"bounds_wgs84.xmax").count() shouldBe (0)
      catalog.filter($"bounds_wgs84.ymin" > $"bounds_wgs84.ymax").count() shouldBe (0)

      val geo_area_row = spark.sql(
        """
           SELECT min(st_area(st_geometry(bounds_wgs84))) AS area
           FROM l8_catalog
           WHERE st_intersects(st_geometry(bounds_wgs84), st_geomFromText('LINESTRING(-78.035 39.004,-80.166 37.241)')) AND
          acquisition_date > to_timestamp('2017-11-01') AND
          acquisition_date <= to_timestamp('2017-11-16')
        """).first()
      val geo_area = geo_area_row.getDouble(0)
      geo_area shouldBe < (6.5)
      geo_area shouldBe > (4.5)
    }
  }

  describe("Read L8 scenes from PDS") {
    it("should be compatible with raster DataSource") {
      val df = spark.read.raster
        .fromCatalog(scenes, "B1", "B3")
        .withTileDimensions(512, 512)
        .load()

      // Further refine down to a tile
      val sub = df.select($"B3")
        .where(st_contains(st_geometry(rf_extent($"B1")), st_makePoint(574965, 7679175)))

      val stats = sub.select(rf_agg_stats($"B3")).first

      stats.data_cells should be (512L * 512L)
      stats.mean shouldBe > (10000.0)
    }

    it("should construct an RGB composite") {
      val aoi = Extent(31.115, 29.963, 31.148, 29.99)
      val scene = catalog
        .where(
          to_date($"acquisition_date") === to_date(lit("2019-07-03")) &&
            st_intersects(st_geometry($"bounds_wgs84"), geomLit(aoi.jtsGeom))
        )
        .orderBy("cloud_cover_pct")
        .limit(1)

      val df = spark.read.raster
        .fromCatalog(scene, "B4", "B3", "B2")
        .withTileDimensions(256, 256)
        .load()
        .where(st_contains(rf_geometry($"B4"), st_reproject(geomLit(aoi.jtsGeom), lit("EPSG:4326"), rf_crs($"B4"))))


      val raster = df.aggregateRaster(LatLng, aoi, None)
      println(raster)

//      import geotrellis.raster.io.geotiff.{GeoTiffOptions, MultibandGeoTiff, Tiled}
//      import geotrellis.raster.io.geotiff.compression.{DeflateCompression}
//      import geotrellis.raster.io.geotiff.tags.codes.ColorSpace
//      val tiffOptions = GeoTiffOptions(Tiled,  DeflateCompression, ColorSpace.RGB)
//      MultibandGeoTiff(raster, raster.crs, tiffOptions).write("target/composite.tif")
    }
  }
}
