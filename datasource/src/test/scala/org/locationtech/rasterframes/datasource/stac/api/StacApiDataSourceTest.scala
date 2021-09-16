/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2021 Astraea, Inc.
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

package org.locationtech.rasterframes.datasource.stac.api

import org.locationtech.rasterframes.datasource.raster._
import org.locationtech.rasterframes.datasource.stac.api.encoders._
import com.azavea.stac4s.StacItem
import com.azavea.stac4s.api.client.{SearchFilters, SttpStacClient}
import cats.syntax.option._
import cats.effect.IO
import eu.timepit.refined.auto._
import geotrellis.store.util.BlockingThreadPool
import org.apache.spark.sql.functions.explode
import org.locationtech.rasterframes.TestEnvironment
import sttp.client3.asynchttpclient.cats.AsyncHttpClientCatsBackend
import sttp.client3.UriContext

class StacApiDataSourceTest extends TestEnvironment { self =>

  describe("STAC API spark reader") {
    it("should read items from Franklin service") {
      import spark.implicits._

      val results =
        spark
          .read
          .stacApi(
            "https://franklin.nasa-hsi.azavea.com/",
            filters = SearchFilters(items = List("aviris-l1-cogs_f130329t01p00r06_sc01")),
            searchLimit = Some(1)
          ).load

      results.rdd.partitions.length shouldBe 1
      results.count() shouldBe 1L

      results.as[StacItem].head.id shouldBe "aviris-l1-cogs_f130329t01p00r06_sc01"

      val ddf = results.select($"id", explode($"assets"))

      ddf.printSchema()
      ddf.show

      ddf
        .select($"id", $"value.href" as "band")
        .as[(String, String)]
        .first() shouldBe (
          "aviris-l1-cogs_f130329t01p00r06_sc01",
          "s3://aviris-data/aviris-scene-cogs-l2/2013/f130329t01p00r06/f130329t01p00r06rdn_e_sc01_ort_img_tiff.tiff"
        )
    }

    // requires AWS credentials
    // TODO: make a public test
    ignore("should load COGs from Franklin service no syntax") {
      import spark.implicits._

      val results =
        spark
          .read
          .stacApi(
            "https://franklin.nasa-hsi.azavea.com/",
            filters = SearchFilters(items = List("aviris-l1-cogs_f130329t01p00r06_sc01")),
            searchLimit = Some(1)
          ).load

      results.rdd.partitions.length shouldBe 1

      results.as[StacItem].first().id shouldBe "aviris-l1-cogs_f130329t01p00r06_sc01"

      val assets =
        results
          .select($"id", explode($"assets"))
          .select($"value.href" as "band")

      assets.printSchema()
      assets.show

      val rasters =
        spark
          .read
          .raster
          .fromCatalog(assets, "band")
          .withTileDimensions(128, 128)
          .withBandIndexes(0)
          .load()

      rasters.printSchema()

      println("--- Loading ---")
      rasters.count() shouldBe 4182L
    }

    // requires AWS credentials
    // TODO: make a public test
    ignore("should load COGs from Franklin service using syntax") {
      import spark.implicits._
      val items =
        spark
          .read
          .stacApi(
            "https://franklin.nasa-hsi.azavea.com/",
            filters = SearchFilters(items = List("aviris-l1-cogs_f130329t01p00r06_sc01")),
            searchLimit = Some(1)
          )
          .loadStac

      val assets =
        items
          .flattenAssets
          .select($"asset.href" as "band")

      assets.schema
      assets.show

      val rasters =
        spark
          .read
          .raster
          .fromCatalog(assets, "band")
          .withTileDimensions(128, 128)
          .withBandIndexes(0)
          .load()

      rasters.printSchema()

      println("--- Loading ---")
      rasters.count() shouldBe 4182L
    }

    it("should read from Astraea Earth service") {
      import spark.implicits._

      val results = spark.read.stacApi("https://eod-catalog-svc-prod.astraea.earth/", searchLimit = Some(1)).load

      // results.printSchema()

      results.rdd.partitions.length shouldBe 1
      results.count() shouldBe 1

      results.as[StacItem].first().id shouldBe "S2A_OPER_MSI_L2A_TL_EPAE_20190527T094026_A020508_T46VCQ_N02.12"

      val ddf = results.select($"id", explode($"assets"))

      ddf.printSchema()

      val assets = ddf.select($"id", $"value.href" as "band")

      assets.printSchema()
      assets.show

      assets.as[(String, String)].first() shouldBe (
        "S2A_OPER_MSI_L2A_TL_EPAE_20190527T094026_A020508_T46VCQ_N02.12",
        "s3://sentinel-s2-l2a/tiles/46/V/CQ/2019/5/27/0/R60m/B03.jp2"
      )
    }

    ignore("should fetch rasters from Astraea STAC API service") {
      import spark.implicits._
      val items =
        spark
          .read
          .stacApi("https://eod-catalog-svc-prod.astraea.earth/", searchLimit = 1.some)
          .load

      println(items.collect().toList.length)

      val assets = items.select($"id", explode($"assets")).select($"value.href" as "band").limit(1)

      val rasters = spark.read.raster
        .fromCatalog(assets, "band")
        .withTileDimensions(128, 128)
        .withBandIndexes(0)
        .load()

      rasters.printSchema()

      println("--- Loading ---")
      info(rasters.count().toString)
    }

    ignore("should fetch rasters from the Datacube STAC API service") {
      import spark.implicits._
      val items = spark.read.stacApi("https://datacube.services.geo.ca/api",  filters = SearchFilters(collections=List("markham")), searchLimit = Some(1)).load

      println(items.collect().toList.length)

      val assets = items.select($"id", explode($"assets")).select($"value.href" as "band").limit(1)

      val rasters = spark.read.raster.fromCatalog(assets, "band").withTileDimensions(1024, 1024).withBandIndexes(0).load()

      rasters.printSchema()

      println("--- Loading ---")
      info(rasters.count().toString)
    }
  }

  it("STAC API Client should query Astraea STAC API") {
    import spark.implicits._

    implicit val cs = IO.contextShift(BlockingThreadPool.executionContext)
    val realitems: List[StacItem] = AsyncHttpClientCatsBackend
      .resource[IO]()
      .use { backend =>
        SttpStacClient(backend, uri"https://eod-catalog-svc-prod.astraea.earth/")
          .search(SearchFilters(items = List("S2A_OPER_MSI_L2A_TL_EPAE_20190527T094026_A020508_T46VCQ_N02.12")))
          .take(1)
          .compile
          .toList
      }
      .unsafeRunSync()

    sc
      .parallelize(realitems)
      .toDF()
      .as[StacItem]
      .first().id shouldBe "S2A_OPER_MSI_L2A_TL_EPAE_20190527T094026_A020508_T46VCQ_N02.12"
  }
}
