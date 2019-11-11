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
package org.locationtech.rasterframes.datasource.geotiff

import java.nio.file.{Path, Paths}

import geotrellis.proj4._
import geotrellis.raster.CellType
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import geotrellis.vector.Extent
import org.locationtech.rasterframes._
import org.apache.spark.sql.functions._
import org.locationtech.rasterframes.TestEnvironment
import org.locationtech.rasterframes.datasource.raster._

/**
 * @since 1/14/18
 */
class GeoTiffDataSourceSpec
    extends TestEnvironment with TestData {

  describe("GeoTiff reading") {

    it("should read sample GeoTiff") {
      val rf = spark.read.format("geotiff").load(cogPath.toASCIIString).asLayer

      assert(rf.count() > 10)
    }

    it("should lay out tiles correctly") {

      val rf = spark.read.format("geotiff").load(cogPath.toASCIIString).asLayer

      val tlm = rf.tileLayerMetadata.left.get
      val gb = tlm.gridBounds
      assert(gb.colMax > gb.colMin)
      assert(gb.rowMax > gb.rowMin)
    }

    it("should lay out tiles correctly for non-tiled tif") {
      val rf = spark.read.format("geotiff").load(nonCogPath.toASCIIString).asLayer

      assert(rf.count() > 1)

      import org.apache.spark.sql.functions._
      logger.info(
        rf.agg(
          min(col("spatial_key.row")) as "rowmin",
          max(col("spatial_key.row")) as "rowmax",
          min(col("spatial_key.col")) as "colmin",
          max(col("spatial_key.col")) as "colmax"

        ).first.toSeq.toString()
      )
      val tlm = rf.tileLayerMetadata.left.get
      val gb = tlm.gridBounds
      assert(gb.rowMax > gb.rowMin)
      assert(gb.colMax > gb.colMin)

    }

    it("should read in correctly check-summed contents") {
      // c.f. TileStatsSpec -> computing statistics over tiles -> should compute tile statistics -> sum
      val rf = spark.read.format("geotiff").load(l8B1SamplePath.toASCIIString).asLayer
      val expected = 309149454 // computed with rasterio
      val result = rf.agg(
        sum(rf_tile_sum(rf("tile")))
      ).collect().head.getDouble(0)

      assert(result === expected)
    }
  }

  describe("GeoTiff writing") {

    def checkTiff(file: Path, cols: Int, rows: Int, extent: Extent, cellType: Option[CellType] = None) = {
      val outputTif = SinglebandGeoTiff(file.toString)
      outputTif.tile.dimensions should be ((cols, rows))
      outputTif.extent should be (extent)
      cellType.foreach(ct =>
        outputTif.cellType should be (ct)
      )
    }

    it("should write GeoTIFF RF to parquet") {
      val rf = spark.read.format("geotiff").load(cogPath.toASCIIString).asLayer
      assert(write(rf))
    }

    it("should write GeoTIFF from layer") {
      val rf = spark.read.format("geotiff").load(cogPath.toASCIIString).asLayer

      logger.info(s"Read extent: ${rf.tileLayerMetadata.merge.extent}")

      val out = Paths.get("target", "example-geotiff.tif")
      logger.info(s"Writing to $out")
      noException shouldBe thrownBy {
        rf.write.format("geotiff").save(out.toString)
      }
      val extent = rf.tileLayerMetadata.merge.extent

      checkTiff(out, 1028, 989, extent)
    }

    it("should write unstructured raster") {
      import spark.implicits._
      val df = spark.read.format("raster")
        .option("tileDimensions", "32,32")  // oddball
        .load(nonCogPath.toASCIIString) // core L8-B8-Robinson-IL.tiff

      df.count() should be > 0L

      val crs = df.select(rf_crs($"proj_raster")).first()

      val out = Paths.get("target", "unstructured.tif")

      noException shouldBe thrownBy {
        df.write.geotiff.withCRS(crs).save(out.toString)
      }

      val (inCols, inRows) = {
        val id = sampleGeoTiff.imageData // inshallah same as nonCogPath
        (id.cols, id.rows)
      }
      inCols should be (774)
      inRows should be (500) //from gdalinfo

      checkTiff(out, inCols, inRows, Extent(431902.5, 4313647.5, 443512.5, 4321147.5))
    }

    it("should round trip unstructured raster from COG"){
      import spark.implicits._
      import org.locationtech.rasterframes.datasource.raster._

      val df = spark.read.raster.withTileDimensions(64, 64).load(singlebandCogPath.toASCIIString)

      val resourceCols = 963 // from gdalinfo
      val resourceRows = 754
      val resourceExtent = Extent(752325.0, 3872685.0, 781215.0, 3895305.0)

      df.count() should be > 0L

      val crs = df.select(rf_crs(col("proj_raster"))).first()

      val totalExtentRow = df.select(rf_extent($"proj_raster").alias("ext"))
        .agg(
          min($"ext.xmin").alias("xmin"),
          min($"ext.ymin").alias("ymin"),
          max($"ext.xmax").alias("xmax"),
          max($"ext.ymax").alias("ymax")
        ).first()

      val dfExtent = Extent(totalExtentRow.getDouble(0), totalExtentRow.getDouble(1), totalExtentRow.getDouble(2), totalExtentRow.getDouble(3))
      logger.info(s"Dataframe extent: ${dfExtent.toString()}")

      dfExtent shouldBe resourceExtent

      val out = Paths.get("target", "unstructured_cog.tif")

      noException shouldBe thrownBy {
        df.write.geotiff.withCRS(crs).save(out.toString)
      }

      val (inCols, inRows, inExtent, inCellType) = {
        val tif = readSingleband("LC08_B7_Memphis_COG.tiff")
        val id = tif.imageData
        (id.cols, id.rows, tif.extent, tif.cellType)
      }
      inCols should be (resourceCols)
      inRows should be (resourceRows) //from gdalinfo
      inExtent should be (resourceExtent)

      checkTiff(out, inCols, inRows, resourceExtent, Some(inCellType))
    }

    it("should write GeoTIFF without layer") {
      val pr = col("proj_raster_b0")

      val sample = rgbCogSample
      val expectedExtent = sample.extent
      val (expCols, expRows) = sample.tile.dimensions

      val rf = spark.read.raster.withBandIndexes(0, 1, 2).load(rgbCogSamplePath.toASCIIString)

      withClue("extent/crs columns provided") {
        val out = Paths.get("target", "example2a-geotiff.tif")
        noException shouldBe thrownBy {
          rf
            .withColumn("extent", rf_extent(pr))
            .withColumn("crs", rf_crs(pr))
            .write.geotiff.withCRS(sample.crs).save(out.toString)
          checkTiff(out, expCols, expRows, expectedExtent, Some(sample.cellType))
        }
      }

      withClue("without extent/crs columns") {
        val out = Paths.get("target", "example2b-geotiff.tif")
        noException shouldBe thrownBy {
          rf
            .write.geotiff.withCRS(sample.crs).save(out.toString)
          checkTiff(out, expCols, expRows, expectedExtent, Some(sample.cellType))
        }
      }

      withClue("with downsampling") {
        val out = Paths.get("target", "example2c-geotiff.tif")
        noException shouldBe thrownBy {
          rf
            .write.geotiff
            .withCRS(LatLng)
            .withDimensions(128, 128)
            .save(out.toString)
        }
      }
    }

    it("should produce the correct subregion from layer") {
      import spark.implicits._
      val rf = SinglebandGeoTiff(TestData.singlebandCogPath.getPath)
        .projectedRaster.toLayer(128, 128).withExtent()

      val out = Paths.get("target", "example3-geotiff.tif")
      logger.info(s"Writing to $out")

      val bitOfLayer = rf.filter($"spatial_key.col" === 0 && $"spatial_key.row" === 0)
      val expectedExtent = bitOfLayer.select($"extent".as[Extent]).first()
      bitOfLayer.write.geotiff.save(out.toString)

      checkTiff(out, 128, 128, expectedExtent)
    }

    it("should produce the correct subregion without layer") {
      import spark.implicits._

      val rf = spark.read.raster
        .withTileDimensions(128, 128)
        .load(TestData.singlebandCogPath.toASCIIString)

      val out = Paths.get("target", "example3-geotiff.tif")
      logger.info(s"Writing to $out")

      val bitOfLayer = rf.filter(st_intersects(st_makePoint(754245, 3893385), rf_geometry($"proj_raster")))
      val expectedExtent = bitOfLayer.select(rf_extent($"proj_raster")).first()
      val crs = bitOfLayer.select(rf_crs($"proj_raster")).first()
      bitOfLayer.write.geotiff.withCRS(crs).save(out.toString)

      checkTiff(out, 128, 128, expectedExtent)
    }

    def s(band: Int): String =
      s"https://modis-pds.s3.amazonaws.com/MCD43A4.006/11/08/2019059/" +
        s"MCD43A4.A2019059.h11v08.006.2019072203257_B0${band}.TIF"

    it("shoud write multiband") {
      import org.locationtech.rasterframes.datasource.raster._

      val cat = s"""
red,green,blue
${s(1)},${s(4)},${s(3)}
"""
      val scene = spark.read.raster.fromCSV(cat, "red", "green", "blue").load()
      val out = Paths.get("target", "geotiff-overview.tif").toString
      scene.write.geotiff
        .withCRS(LatLng)
        .withDimensions(256, 256)
        .save(out)

      val outTif = MultibandGeoTiff(out)
      outTif.bandCount should be (3)
    }
  }
}
