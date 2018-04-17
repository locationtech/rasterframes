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

package astraea.spark.rasterframes.experimental.datasource.awspds
import astraea.spark.rasterframes.TestEnvironment

/**
 *
 *
 * @since 5/4/18
 */
class MODISCatalogRelationTest extends TestEnvironment {
  describe("Representing MODIS scenes as a Spark data source") {
    import spark.implicits._
    it("should provide a non-empty catalog") {
      val catalog = spark.read.format(MODISCatalogDataSource.NAME).load()
      val scenes = catalog.filter($"tileId".contains("h11v12"))
      scenes.show(false)
      assert(scenes.count() === 1)
    }

    it("should construct band specific download URLs") {
      val catalog = spark.read.format(MODISCatalogDataSource.NAME).load()
      val b01 = catalog.limit(1).select(modis_band_url("B01"))
      b01.show(false)
    }

    it("should download geotiff as blob") {
      import org.apache.spark.sql.functions.{length ⇒ alength, _}
      val catalog = spark.read.format(MODISCatalogDataSource.NAME).load()
      val b01 = catalog.limit(1).select(download(modis_band_url("B01")))
        .withColumn("B01_size", alength(col("B01_data")))
      b01.printSchema
      b01.show(true)
    }

    it("should download geotiff as tiles") {
      val catalog = spark.read.format(MODISCatalogDataSource.NAME).load()
      val b01 = catalog
        .select($"*", download_tiles(modis_band_url("B01")))
        .where($"tileId".contains("h24v03"))
      b01.printSchema()
      b01.show(200)
      //b01.select("B01_spatial_key", "B01_tile").show(200, false)
    }
  }
}
