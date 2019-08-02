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

import java.nio.file.Paths

import geotrellis.proj4._
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import geotrellis.vector.Extent
import org.locationtech.rasterframes._
import org.apache.spark.sql.functions._
import org.locationtech.rasterframes.TestEnvironment

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

    it("should lay out tiles correctly"){

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
    }

    it("should write unstructured raster") {
      import spark.implicits._
      val df = spark.read.format("raster")
        .option("tileDimensions", "32,32")  // oddball
        .load(nonCogPath.toASCIIString) // core L8-B8-Robinson-IL.tiff

      df.count() should be > 0L

      val crs = df.select(rf_crs($"proj_raster")).first()

      noException shouldBe thrownBy {
        df.write.geotiff.withCRS(crs).save("unstructured.tif")
      }

      val (inCols, inRows) = {
        val id = sampleGeoTiff.imageData // inshallah same as nonCogPath
        (id.cols, id.rows)
      }
      inCols should be (774)
      inRows should be (500) //from gdalinfo

      val outputTif = SinglebandGeoTiff("unstructured.tif")
      outputTif.imageData.cols should be (inCols)
      outputTif.imageData.rows should be (inRows)

      // TODO check datatype, extent.
    }

    it("should round trip unstructured raster from COG"){
      val df = spark.read.format("raster")
        .load(getClass.getResource("/LC08_B7_Memphis_COG.tiff").toURI.toASCIIString())

      df.count() should be > 0L

      val crs = df.select(rf_crs(col("proj_raster"))).first()

      noException shouldBe thrownBy {
        df.write.geotiff.withCRS(crs).save("target/unstructured_cog.tif")
      }

      val (inCols, inRows, inExtent, inCellType) = {
        val tif = readSingleband("LC08_B7_Memphis_COG.tiff")
        val id = tif.imageData
        (id.cols, id.rows, tif.extent, tif.cellType)
      }
      inCols should be (963)
      inRows should be (754) //from gdalinfo
      inExtent should be (Extent(752325.0, 3872685.0, 781215.0, 3895305.0))

      val outputTif = SinglebandGeoTiff("target/unstructured_cog.tif")
      outputTif.imageData.cols should be (inCols)
      outputTif.imageData.rows should be (inRows)
      outputTif.extent should be (inExtent)
      outputTif.cellType should be (inCellType)

    }

    /*
    it("should round trip jasons favorite unstructured raster round trip okay") {
      import org.locationtech.rasterframes.datasource.raster._
      import spark.implicits._

      val jasonsRasterPath = "https://modis-pds.s3.amazonaws.com/MCD43A4.006/17/03/2019193/" +
                            "MCD43A4.A2019193.h17v03.006.2019202033615_B06.TIF"
      val df = spark.read.raster
          .withTileDimensions(233, 133)
          .from(Seq(jasonsRasterPath))
        .load()

      logger.debug("Read local file metadata")
      val (inCols, inRows) = {
        val in = readSingleband("MCD43A4.A2019193.h17v03.006.2019202033615_B06.TIF")
        (in.cols, in.rows)
      }
      inCols should be (2400)  // from GDAL
      inRows should be (2400)  // from GDAL

      val outPath = "datasources/target/rf_mcd43a4.A2019193.h17v03.tif"

      // now take actions on the read df
      logger.debug("Actions on raster ref dataframe")
      df.count() should be > 100L
      val crs = df.select(rf_crs($"proj_raster")).first()
      logger.debug("Write full res geotiff from dataframe")
      df.write.geotiff.withCRS(crs).save(outPath)

      // compare written file to path
      logger.debug("Inspect written geotiff metadata")
      val (outCols, outRows) = {
        val outputTif = SinglebandGeoTiff(outPath)
        (outputTif.cols, outputTif.rows)
      }
      outCols should be (inCols)
      outRows should be (inRows)
      // todo check extent and crs just for grins.

    }*/

    it("should write GeoTIFF without layer") {
      import org.locationtech.rasterframes.datasource.raster._
      val pr = col("proj_raster_b0")
      val rf = spark.read.raster.withBandIndexes(0, 1, 2).load(rgbCogSamplePath.toASCIIString)

      val out = Paths.get("target", "example2-geotiff.tif")
      logger.info(s"Writing to $out")

      withClue("explicit extent/crs") {
        noException shouldBe thrownBy {
          rf
            .withColumn("extent", rf_extent(pr))
            .withColumn("crs", rf_crs(pr))
            .write.geotiff.withCRS(LatLng).save(out.toString)
        }
      }

      withClue("without explicit extent/crs") {
        noException shouldBe thrownBy {
          rf
            .write.geotiff.withCRS(LatLng).save(out.toString)
        }
      }
      withClue("with downsampling") {
        noException shouldBe thrownBy {
          rf
            .write.geotiff
            .withCRS(LatLng)
            .withDimensions(128, 128)
            .save(out.toString)
        }
      }
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
      scene.write.geotiff
        .withCRS(LatLng)
        .withDimensions(256, 256)
        .save("geotiff-overview.tif")

      val outTif = MultibandGeoTiff("geotiff-overview.tif")
      outTif.bandCount should be (3)
    }
  }
}
