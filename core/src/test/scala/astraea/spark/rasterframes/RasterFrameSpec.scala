

package astraea.spark.rasterframes

import java.sql.Timestamp
import java.time.ZonedDateTime

import geotrellis.proj4.LatLng
import geotrellis.raster.render.{ColorMap, ColorRamp}
import geotrellis.raster.{ProjectedRaster, Tile, TileFeature, TileLayout}
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.tiling._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import astraea.spark.rasterframes.util._

import scala.util.control.NonFatal

/**
 * RasterFrame test rig.
 *
 * @since 7/10/17
 */
class RasterFrameSpec extends TestEnvironment with MetadataKeys
  with TestData with IntelliJPresentationCompilerHack {
  import TestData.randomTile
  import spark.implicits._

  describe("Runtime environment") {
    it("should provide build info") {
      assert(RFBuildInfo.toMap.nonEmpty)
      assert(RFBuildInfo.toString.nonEmpty)
    }
    it("should provide Spark initialization methods") {
      assert(spark.withRasterFrames.isInstanceOf[SparkSession])
      assert(sqlContext.withRasterFrames.isInstanceOf[SQLContext])
    }
  }

  describe("DataFrame") {
    it("should support column prefixes") {
      val baseDF = Seq((1, "one", 1.0), (2, "two", 2.0)).toDF("int", "str", "flt")

      val df1 = baseDF.withPrefixedColumnNames("ONE_")
      val df2 = baseDF.withPrefixedColumnNames("TWO_")

      assert(df1.columns.forall(_.startsWith("ONE_")))
      assert(df2.columns.forall(_.startsWith("TWO_")))
      assert(df1.join(df2, $"ONE_int" === $"TWO_int").columns === df1.columns ++ df2.columns)
    }
  }

  describe("RasterFrame") {
    it("should implicitly convert from spatial layer type") {

      val tileLayerRDD = TestData.randomSpatialTileLayerRDD(20, 20, 2, 2)

      val rf = tileLayerRDD.toRF

      assert(rf.tileColumns.nonEmpty)
      assert(rf.spatialKeyColumn.columnName == "spatial_key")

      assert(rf.schema.head.metadata.contains(CONTEXT_METADATA_KEY))
      assert(rf.schema.head.metadata.json.contains("tileLayout"))

      assert(
        rf.select(tileDimensions($"tile"))
          .as[Tuple1[(Int, Int)]]
          .map(_._1)
          .collect()
          .forall(_ == (10, 10))
      )

      assert(rf.count() === 4)
    }

    it("should implicitly convert from spatiotemporal layer type") {

      val tileLayerRDD = TestData.randomSpatioTemporalTileLayerRDD(20, 20, 2, 2)

      val rf = tileLayerRDD.toRF

      //rf.printSchema()
      //rf.show()
      try {
        assert(rf.tileColumns.nonEmpty)
        assert(rf.spatialKeyColumn.columnName === "spatial_key")
        assert(rf.temporalKeyColumn.map(_.columnName) === Some("temporal_key"))
      }
      catch {
        case NonFatal(ex) ⇒
          rf.printSchema()
          println(rf.schema.prettyJson)
          throw ex
      }
    }

    it("should implicitly convert layer of TileFeature") {
      val tile = TileFeature(randomTile(20, 20, "uint8"), (1, "b", 3.0))

      val tileLayout = TileLayout(1, 1, 20, 20)

      val layoutScheme = FloatingLayoutScheme(tileLayout.tileCols, tileLayout.tileRows)
      val inputRdd = sc.parallelize(Seq((ProjectedExtent(LatLng.worldExtent, LatLng), tile)))

      val (_, metadata) = inputRdd.collectMetadata[SpatialKey](LatLng, layoutScheme)

      val tileRDD = inputRdd.map {case (k, v) ⇒ (metadata.mapTransform(k.extent.center), v)}

      val tileLayerRDD = TileFeatureLayerRDD(tileRDD, metadata)

      val rf = tileLayerRDD.toRF

      assert(rf.columns.toSet === Set(SPATIAL_KEY_COLUMN, TILE_COLUMN, TILE_FEATURE_DATA_COLUMN).map(_.columnName))
    }

    it("should implicitly convert spatiotemporal layer of TileFeature") {
      val tile = TileFeature(randomTile(20, 20, "uint8"), (1, "b", 3.0))

      val tileLayout = TileLayout(1, 1, 20, 20)

      val layoutScheme = FloatingLayoutScheme(tileLayout.tileCols, tileLayout.tileRows)
      val inputRdd = sc.parallelize(Seq((TemporalProjectedExtent(LatLng.worldExtent, LatLng, ZonedDateTime.now()), tile)))

      val (_, metadata) = inputRdd.collectMetadata[SpaceTimeKey](LatLng, layoutScheme)

      val tileRDD = inputRdd.map {case (k, v) ⇒ (SpaceTimeKey(metadata.mapTransform(k.extent.center), k.time), v)}

      val tileLayerRDD = TileFeatureLayerRDD(tileRDD, metadata)

      val rf = tileLayerRDD.toRF

      assert(rf.columns.toSet === Set(SPATIAL_KEY_COLUMN, TEMPORAL_KEY_COLUMN, TILE_COLUMN, TILE_FEATURE_DATA_COLUMN).map(_.columnName))
    }

    it("should support adding a timestamp column") {
      val now = ZonedDateTime.now()
      val rf = sampleGeoTiff.projectedRaster.toRF(256, 256)
      val wt = rf.addTemporalComponent(now)
      val goodie = wt.withTimestamp()
      assert(goodie.columns.contains("timestamp"))
      assert(goodie.count > 0)
      val ts = goodie.select(col("timestamp").as[Timestamp]).first

      assert(ts === Timestamp.from(now.toInstant))
    }

    it("should support spatial joins") {
      val rf = sampleGeoTiff.projectedRaster.toRF(256, 256)

      val wt = rf.addTemporalComponent(TemporalKey(34))

      assert(wt.columns.contains(TEMPORAL_KEY_COLUMN.columnName))

      val joined = wt.spatialJoin(wt, "outer")
      joined.printSchema

      // Should be both left and right column names.
      assert(joined.columns.count(_.contains(TEMPORAL_KEY_COLUMN.columnName)) === 2)
      assert(joined.columns.count(_.contains(SPATIAL_KEY_COLUMN.columnName)) === 2)
    }

    it("should have correct schema on inner spatial joins") {
      val left = sampleGeoTiff.projectedRaster.toRF(256, 256)
        .addTemporalComponent(TemporalKey(34))

      val right = left.withColumnRenamed(left.tileColumns.head.columnName, "rightTile")
        .asRF

      val joined = left.spatialJoin(right, "inner")
      joined.printSchema

      // Should use left's key column names
      assert(joined.spatialKeyColumn.columnName === left.spatialKeyColumn.columnName)
      assert(joined.temporalKeyColumn.map(_.columnName) === left.temporalKeyColumn.map(_.columnName))
      // since right is a copy of left, should not drop any rows with inner join
      assert(joined.count === left.count)

    }

    it("should convert a GeoTiff to RasterFrame") {
      val praster: ProjectedRaster[Tile] = sampleGeoTiff.projectedRaster
      val (cols, rows) = praster.raster.dimensions

      val layoutCols = math.ceil(cols / 128.0).toInt
      val layoutRows = math.ceil(rows / 128.0).toInt

      assert(praster.toRF.count() === 1)
      assert(praster.toRF(128, 128).count() === (layoutCols * layoutRows))
    }

    it("should provide TileLayerMetadata[SpatialKey]") {
      val rf = sampleGeoTiff.projectedRaster.toRF(256, 256)
      val tlm = rf.tileLayerMetadata.widen
      val bounds = tlm.bounds.get
      assert(bounds === KeyBounds(SpatialKey(0, 0), SpatialKey(3, 1)))
    }

    it("should provide TileLayerMetadata[SpaceTimeKey]") {
      val now = ZonedDateTime.now()
      val rf = sampleGeoTiff.projectedRaster.toRF(256, 256, now)
      val tlm = rf.tileLayerMetadata.widen
      val bounds = tlm.bounds.get
      assert(bounds._1 === SpaceTimeKey(0, 0, now))
      assert(bounds._2 === SpaceTimeKey(3, 1, now))
    }

    it("should clip TileLayerMetadata extent") {
      val tiled = sampleTileLayerRDD

      val rf = tiled.toRF

      val worldish = Extent(-179, -89, 179, 89)
      val areaish = Extent(-90, 30, -81, 39)

      val orig = tiled.metadata.extent
      assert(orig.contains(worldish))
      assert(orig.contains(areaish))

      val clipped = rf.clipLayerExtent.tileLayerMetadata.widen.extent
      assert(!clipped.contains(worldish))
      assert(clipped.contains(areaish))
    }

    def basicallySame(expected: Extent, computed: Extent): Unit = {
      val components = Seq(
        (expected.xmin, computed.xmin),
        (expected.ymin, computed.ymin),
        (expected.xmax, computed.xmax),
        (expected.ymax, computed.ymax)
      )
      forEvery(components)(c ⇒
        assert(c._1 === c._2 +- 0.000001)
      )
    }

    it("shouldn't clip already clipped extents") {
      val rf = TestData.randomSpatialTileLayerRDD(1024, 1024, 8, 8).toRF

      val expected = rf.tileLayerMetadata.widen.extent
      val computed = rf.clipLayerExtent.tileLayerMetadata.widen.extent
      basicallySame(expected, computed)

      val pr = sampleGeoTiff.projectedRaster
      val rf2 = pr.toRF(256, 256)
      val expected2 = rf2.tileLayerMetadata.widen.extent
      val computed2 = rf2.clipLayerExtent.tileLayerMetadata.widen.extent
      basicallySame(expected2, computed2)
    }

    def Greyscale(stops: Int): ColorRamp = {
      val colors = (0 to stops)
        .map(i ⇒ {
          val c = java.awt.Color.HSBtoRGB(0f, 0f, i / stops.toFloat)
          (c << 8) | 0xFF // Add alpha channel.
        })
      ColorRamp(colors)
    }

    def render(tile: Tile, tag: String): Unit = {
      if(false && !isCI) {
        val colors = ColorMap.fromQuantileBreaks(tile.histogram, Greyscale(128))
        val path = s"target/${getClass.getSimpleName}_$tag.png"
        logger.info(s"Writing '$path'")
        tile.color(colors).renderPng().write(path)
      }
    }

    it("should rasterize with a spatiotemporal key") {
      val rf = TestData.randomSpatioTemporalTileLayerRDD(20, 20, 2, 2).toRF

      val md = rf.schema.fields(0).metadata

      //println(rf.extract[TileLayerMetadata[SpaceTimeKey]](CONTEXT_METADATA_KEY)(md))

      rf.toRaster($"tile", 128, 128)
    }

    it("should maintain metadata after all spatial join operations") {
      val rf1 = TestData.randomSpatioTemporalTileLayerRDD(20, 20, 2, 2).toRF
      val rf2 = TestData.randomSpatioTemporalTileLayerRDD(20, 20, 2, 2).toRF

      val joinTypes = Seq("inner", "outer", "fullouter", "left_outer", "right_outer", "leftsemi")
      forEvery(joinTypes) { jt ⇒
        val joined = rf1.spatialJoin(rf2, jt)
        //println(joined.schema.json)
        assert(joined.tileLayerMetadata.isRight)
      }
    }

    it("should restitch to raster") {
      // 774 × 500
      val praster: ProjectedRaster[Tile] = sampleGeoTiff.projectedRaster
      val (cols, rows) = praster.raster.dimensions
      val rf = praster.toRF(64, 64)
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
      val rf = praster.toRF(64, 64)

      val equalizer = udf((t: Tile) => t.equalize())

      val equalized = rf.select(equalizer($"tile") as "equalized")

      intercept[IllegalArgumentException] {
        // spatial_key is lost
        equalized.asRF.toRaster($"equalized", 128, 128)
      }
    }

    it("should fetch CRS") {
      val praster: ProjectedRaster[Tile] = sampleGeoTiff.projectedRaster
      val rf = praster.toRF

      assert(rf.crs === praster.crs)
    }
  }
}
