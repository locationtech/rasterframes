

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

import java.net.URI
import java.sql.Timestamp
import java.time.ZonedDateTime

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.{MultibandTile, ProjectedRaster, Raster, Tile, TileFeature, TileLayout, UByteCellType, UByteConstantNoDataCellType}
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SQLContext, SparkSession}
import org.locationtech.rasterframes.model.TileDimensions
import org.locationtech.rasterframes.ref.RasterSource
import org.locationtech.rasterframes.tiles.ProjectedRasterTile
import org.locationtech.rasterframes.util._

import scala.util.control.NonFatal

/**
 * RasterFrameLayer test rig.
 *
 * @since 7/10/17
 */
class RasterLayerSpec extends TestEnvironment with MetadataKeys
  with TestData  {
  import TestData.randomTile
  import spark.implicits._

  describe("Runtime environment") {
    it("should provide build info") {
      assert(RFBuildInfo.toMap.nonEmpty)
      assert(RFBuildInfo.toString.nonEmpty)
    }
    it("should provide Spark initialization methods") {
      assert(spark.withRasterFrames.isInstanceOf[SparkSession])
      assert(spark.sqlContext.withRasterFrames.isInstanceOf[SQLContext])
    }
  }

  describe("DataFrame") {
    it("should support column prefixes") {
      val baseDF = Seq((1, "one", 1.0), (2, "two", 2.0)).toDF("int", "str", "flt")

      val df1 = baseDF.withPrefixedColumnNames("ONE_")
      val df2 = baseDF.withPrefixedColumnNames("TWO_")
      val spark = df1.sparkSession
      import spark.implicits._
      assert(df1.columns.forall(_.startsWith("ONE_")))
      assert(df2.columns.forall(_.startsWith("TWO_")))
      assert(df1.join(df2, $"ONE_int" === $"TWO_int").columns === df1.columns ++ df2.columns)
    }
  }

  describe("RasterFrameLayer") {
    it("should implicitly convert from spatial layer type") {

      val tileLayerRDD = TestData.randomSpatialTileLayerRDD(20, 20, 2, 2)

      val rf = tileLayerRDD.toLayer

      assert(rf.tileColumns.nonEmpty)
      assert(rf.spatialKeyColumn.columnName == "spatial_key")

      assert(rf.schema.head.metadata.contains(CONTEXT_METADATA_KEY))
      assert(rf.schema.head.metadata.json.contains("tileLayout"))

      assert(
        rf.select(rf_dimensions($"tile"))
          .collect()
          .forall(_ == TileDimensions(10, 10))
      )

      assert(rf.count() === 4)

      val cols = tileLayerRDD.toLayer("foo").columns
      assert(!cols.contains("tile"))
      assert(cols.contains("foo"))
    }

    it("should implicitly convert from spatiotemporal layer type") {

      val tileLayerRDD = TestData.randomSpatioTemporalTileLayerRDD(20, 20, 2, 2)

      val rf = tileLayerRDD.toLayer

      try {
        assert(rf.tileColumns.nonEmpty)
        assert(rf.spatialKeyColumn.columnName === "spatial_key")
        assert(rf.temporalKeyColumn.map(_.columnName) === Some("temporal_key"))
      }
      catch {
        case NonFatal(ex) ⇒
          println(rf.schema.prettyJson)
          throw ex
      }
      val cols = tileLayerRDD.toLayer("foo").columns
       assert(!cols.contains("tile"))
       assert(cols.contains("foo"))
    }

    it("should implicitly convert layer of TileFeature") {
      val tile = TileFeature(randomTile(20, 20, UByteCellType), (1, "b", 3.0))

      val tileLayout = TileLayout(1, 1, 20, 20)

      val layoutScheme = FloatingLayoutScheme(tileLayout.tileCols, tileLayout.tileRows)
      val inputRdd = sc.parallelize(Seq((ProjectedExtent(LatLng.worldExtent, LatLng), tile)))

      val (_, metadata) = inputRdd.collectMetadata[SpatialKey](LatLng, layoutScheme)

      val tileRDD = inputRdd.map {case (k, v) ⇒ (metadata.mapTransform(k.extent.center), v)}

      val tileLayerRDD = TileFeatureLayerRDD(tileRDD, metadata)

      val rf = tileLayerRDD.toLayer

      assert(rf.columns.toSet === Set(SPATIAL_KEY_COLUMN, TILE_COLUMN, TILE_FEATURE_DATA_COLUMN).map(_.columnName))
    }

    it("should implicitly convert spatiotemporal layer of TileFeature") {
      val tile = TileFeature(randomTile(20, 20, UByteCellType), (1, "b", 3.0))

      val tileLayout = TileLayout(1, 1, 20, 20)

      val layoutScheme = FloatingLayoutScheme(tileLayout.tileCols, tileLayout.tileRows)
      val inputRdd = sc.parallelize(Seq((TemporalProjectedExtent(LatLng.worldExtent, LatLng, ZonedDateTime.now()), tile)))

      val (_, metadata) = inputRdd.collectMetadata[SpaceTimeKey](LatLng, layoutScheme)

      val tileRDD = inputRdd.map {case (k, v) ⇒ (SpaceTimeKey(metadata.mapTransform(k.extent.center), k.time), v)}

      val tileLayerRDD = TileFeatureLayerRDD(tileRDD, metadata)

      val rf = tileLayerRDD.toLayer

      assert(rf.columns.toSet === Set(SPATIAL_KEY_COLUMN, TEMPORAL_KEY_COLUMN, TILE_COLUMN, TILE_FEATURE_DATA_COLUMN).map(_.columnName))
    }

    it("should support adding a timestamp column") {
      val now = ZonedDateTime.now()
      val rf = sampleGeoTiff.projectedRaster.toLayer(256, 256)
      val wt = rf.addTemporalComponent(now)
      val goodie = wt.withTimestamp()
      assert(goodie.columns.contains("timestamp"))
      assert(goodie.count > 0)
      val ts = goodie.select(col("timestamp").as[Timestamp]).first

      assert(ts === Timestamp.from(now.toInstant))
    }

    it("should support spatial joins") {
      val rf = sampleGeoTiff.projectedRaster.toLayer(256, 256)

      val wt = rf.addTemporalComponent(TemporalKey(34))

      assert(wt.columns.contains(TEMPORAL_KEY_COLUMN.columnName))

      val joined = wt.spatialJoin(wt, "outer")

      // Should be both left and right column names.
      assert(joined.columns.count(_.contains(TEMPORAL_KEY_COLUMN.columnName)) === 2)
      assert(joined.columns.count(_.contains(SPATIAL_KEY_COLUMN.columnName)) === 2)
    }

    it("should have correct schema on inner spatial joins") {
      val left = sampleGeoTiff.projectedRaster.toLayer(256, 256)
        .addTemporalComponent(TemporalKey(34))

      val right = left.withColumnRenamed(left.tileColumns.head.columnName, "rightTile")
        .asLayer

      val joined = left.spatialJoin(right)
      // since right is a copy of left, should not drop any rows with inner join
      assert(joined.count === left.count)

      // Should use left's key column names
      assert(joined.spatialKeyColumn.columnName === left.spatialKeyColumn.columnName)
      assert(joined.temporalKeyColumn.map(_.columnName) === left.temporalKeyColumn.map(_.columnName))
      assert(joined.tileColumns.size === 2)
      assert(joined.notTileColumns.size === 2)
      assert(joined.tileColumns.toSet === joined.tileColumns.toSet)
      assert(joined.tileColumns.toSet !== joined.notTileColumns.toSet)
    }

    it("should convert a GeoTiff to RasterFrameLayer") {
      val praster: ProjectedRaster[Tile] = sampleGeoTiff.projectedRaster
      val (cols, rows) = praster.raster.dimensions

      val layoutCols = math.ceil(cols / 128.0).toInt
      val layoutRows = math.ceil(rows / 128.0).toInt

      assert(praster.toLayer.count() === 1)
      assert(praster.toLayer(128, 128).count() === (layoutCols * layoutRows))
    }

    it("should provide TileLayerMetadata[SpatialKey]") {
      val rf = sampleGeoTiff.projectedRaster.toLayer(256, 256)
      val tlm = rf.tileLayerMetadata.merge
      val bounds = tlm.bounds.get
      assert(bounds === KeyBounds(SpatialKey(0, 0), SpatialKey(3, 1)))
    }

    it("should provide TileLayerMetadata[SpaceTimeKey]") {
      val now = ZonedDateTime.now()
      val rf = sampleGeoTiff.projectedRaster.toLayer(256, 256, now)
      val tlm = rf.tileLayerMetadata.merge
      val bounds = tlm.bounds.get
      assert(bounds._1 === SpaceTimeKey(0, 0, now))
      assert(bounds._2 === SpaceTimeKey(3, 1, now))
    }

    it("should create layer from arbitrary RasterFrame") {
      val src = RasterSource(URI.create("https://raw.githubusercontent.com/locationtech/rasterframes/develop/core/src/test/resources/LC08_RGB_Norfolk_COG.tiff"))
      val srcCrs = src.crs

      def project(r: Raster[MultibandTile]): Seq[ProjectedRasterTile] =
        r.tile.bands.map(b => ProjectedRasterTile(b, r.extent, srcCrs))

      val rasters = src.readAll(bands = Seq(0, 1, 2)).map(project).map(p => (p(0), p(1), p(2)))

      val df = rasters.toDF("red", "green", "blue")

      val crs = CRS.fromString("+proj=utm +zone=18 +datum=WGS84 +units=m +no_defs")

      val extent = Extent(364455.0, 4080315.0, 395295.0, 4109985.0)
      val layout = LayoutDefinition(extent, TileLayout(2, 2, 32, 32))

      val tlm =  new TileLayerMetadata[SpatialKey](
         UByteConstantNoDataCellType,
        layout,
        extent,
        crs,
        KeyBounds(SpatialKey(0, 0), SpatialKey(1, 1))
      )
      val layer = df.toLayer(tlm)

      val TileDimensions(cols, rows) = tlm.totalDimensions
      val prt = layer.toMultibandRaster(Seq($"red", $"green", $"blue"), cols, rows)
      prt.tile.dimensions should be((cols, rows))
      prt.crs should be(crs)
      prt.extent should be(extent)
     }

    it("shouldn't clip already clipped extents") {
      val rf = TestData.randomSpatialTileLayerRDD(1024, 1024, 8, 8).toLayer

      val expected = rf.tileLayerMetadata.merge.extent
      val computed = rf.clipLayerExtent.tileLayerMetadata.merge.extent
      basicallySame(expected, computed)

      val pr = sampleGeoTiff.projectedRaster
      val rf2 = pr.toLayer(256, 256)
      val expected2 = rf2.tileLayerMetadata.merge.extent
      val computed2 = rf2.clipLayerExtent.tileLayerMetadata.merge.extent
      basicallySame(expected2, computed2)
    }

    it("should rasterize with a spatiotemporal key") {
      val rf = TestData.randomSpatioTemporalTileLayerRDD(20, 20, 2, 2).toLayer
      noException shouldBe thrownBy {
        rf.toRaster($"tile", 128, 128)
      }
    }

    it("should maintain metadata after all spatial join operations") {
      val rf1 = TestData.randomSpatioTemporalTileLayerRDD(20, 20, 2, 2).toLayer
      val rf2 = TestData.randomSpatioTemporalTileLayerRDD(20, 20, 2, 2).toLayer

      val joinTypes = Seq("inner", "outer", "fullouter", "left_outer", "right_outer", "leftsemi")
      forEvery(joinTypes) { jt ⇒
        val joined = rf1.spatialJoin(rf2, jt)
        assert(joined.tileLayerMetadata.isRight)
      }
    }

    it("should rasterize multiband") {
      withClue("Landsat") {
        val blue = TestData.l8Sample(1).projectedRaster.toLayer.withRFColumnRenamed("tile", "blue")
        val green = TestData.l8Sample(2).projectedRaster.toLayer.withRFColumnRenamed("tile", "green")
        val red = TestData.l8Sample(3).projectedRaster.toLayer.withRFColumnRenamed("tile", "red")

        val joined = blue.spatialJoin(green).spatialJoin(red)

        noException shouldBe thrownBy {
          val raster = joined.toMultibandRaster(Seq($"red", $"green", $"blue"), 128, 128)
          val png = MultibandRender.Landsat8NaturalColor.render(raster.tile)
          png.write(s"target/L8-${getClass.getSimpleName}.png")
        }
      }
      withClue("NAIP") {
        val red = TestData.naipSample(1).projectedRaster.toLayer.withRFColumnRenamed("tile", "red")
        val green = TestData.naipSample(2).projectedRaster.toLayer.withRFColumnRenamed("tile", "green")
        val blue = TestData.naipSample(3).projectedRaster.toLayer.withRFColumnRenamed("tile", "blue")
        val joined = blue.spatialJoin(green).spatialJoin(red)

        noException shouldBe thrownBy {
          val raster = joined.toMultibandRaster(Seq($"red", $"green", $"blue"), 256, 256)
          val png = MultibandRender.NAIPNaturalColor.render(raster.tile)
          png.write(s"target/NAIP-${getClass.getSimpleName}.png")
        }
      }
    }

    it("should restitch to raster") {
      // 774 × 500
      val praster: ProjectedRaster[Tile] = sampleGeoTiff.projectedRaster
      val (cols, rows) = praster.raster.dimensions
      val rf = praster.toLayer(64, 64)
      val raster = rf.toRaster($"tile", cols, rows)

      render(raster.tile, "normal")
      assert(raster.raster.dimensions ===  (cols, rows))

      val smaller = rf.toRaster($"tile", cols/4, rows/4)
      render(smaller.tile, "smaller")
      assert(smaller.raster.dimensions ===  (cols/4, rows/4))

      val bigger = rf.toRaster($"tile", cols*4, rows*4)
      render(bigger.tile, "bigger")
      assert(bigger.raster.dimensions ===  (cols*4, rows*4))

      val squished = rf.toRaster($"tile", cols*5/4, rows*3/4)
      render(squished.tile, "squished")
      assert(squished.raster.dimensions === (cols*5/4, rows*3/4))
    }

    it("shouldn't restitch raster that's has derived tiles") {
      val praster: ProjectedRaster[Tile] = sampleGeoTiff.projectedRaster
      val rf = praster.toLayer(64, 64)

      val equalizer = udf((t: Tile) => t.equalize())

      val equalized = rf.select(equalizer($"tile") as "equalized")

      intercept[IllegalArgumentException] {
        // spatial_key is lost
        equalized.asLayer.toRaster($"equalized", 128, 128)
      }
    }

    it("should fetch CRS") {
      val praster: ProjectedRaster[Tile] = sampleGeoTiff.projectedRaster
      val rf = praster.toLayer

      assert(rf.crs === praster.crs)
    }
  }
}
