/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017-2019 Azavea & Astraea, Inc.
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
package org.locationtech.rasterframes.datasource.geotrellis

import java.io.File
import java.sql.Timestamp
import java.time.ZonedDateTime
import geotrellis.layer._
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.datasource.DataSourceOptions
import org.locationtech.rasterframes.rules._
import org.locationtech.rasterframes.util._
import geotrellis.proj4.LatLng
import geotrellis.raster._
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.spark._
import geotrellis.spark.store.LayerWriter
import geotrellis.store._
import geotrellis.store.avro.AvroRecordCodec
import geotrellis.store.index.ZCurveKeyIndexMethod
import geotrellis.vector._
import org.apache.avro.generic._
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.hadoop.fs.FileUtil
import org.apache.spark.sql.functions.{udf => sparkUdf}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel
import org.locationtech.rasterframes.TestEnvironment
import org.scalatest.{BeforeAndAfterAll, Inspectors}

import scala.math.{max, min}

class GeoTrellisDataSourceSpec extends TestEnvironment with BeforeAndAfterAll with Inspectors with DataSourceOptions {
  import TestData._

  val tileSize = 12
  lazy val layer = Layer(scratchDir.toUri, LayerId("test-layer", 4))
  lazy val tfLayer = Layer(scratchDir.toUri, LayerId("test-tf-layer", 4))
  lazy val sampleImageLayer = Layer(scratchDir.toUri, LayerId("sample", 0))
  val now = ZonedDateTime.now()
  val tileCoordRange = 2 to 5

  lazy val testRdd = {
    val ct = IntCellType

    //UByteConstantNoDataCellType
    val recs: Seq[(SpaceTimeKey, Tile)] = for {
      col ← tileCoordRange
      row ← tileCoordRange
    } yield SpaceTimeKey(col, row, now) -> TestData.randomTile(tileSize, tileSize, ct)
    val rdd = sc.parallelize(recs)
    val scheme = ZoomedLayoutScheme(LatLng, tileSize = tileSize)
    val layerLayout = scheme.levelForZoom(4).layout
    val layerBounds = KeyBounds(
      SpaceTimeKey(tileCoordRange.start, tileCoordRange.start, now.minusMonths(1)),
      SpaceTimeKey(tileCoordRange.end, tileCoordRange.end, now.plusMonths(1))
    )
    val md = TileLayerMetadata[SpaceTimeKey](
      cellType = ct,
      crs = LatLng,
      bounds = layerBounds,
      layout = layerLayout,
      extent = layerLayout.mapTransform(layerBounds.toGridBounds()))
    ContextRDD(rdd, md)
  }

  override def beforeAll =  {
    super.beforeAll()
    val outputDir = new File(layer.base)
    FileUtil.fullyDelete(outputDir)
    outputDir.deleteOnExit()

    // Test layer writing via RF
    testRdd.toLayer.write.geotrellis.asLayer(layer).save()

    val tfRdd = testRdd.map { case (k, tile) =>
        val md = Map("col" -> k.col,"row" -> k.row)
        (k, TileFeature(tile, md))
    }

    implicit val mdCodec = new AvroRecordCodec[Map[String, Int]]() {
      def schema: Schema = SchemaBuilder.record("metadata")
        .fields()
        .name("map").`type`().map().values().intType().noDefault()
        .endRecord()

      def encode(thing: Map[String, Int], rec: GenericRecord): Unit = {
        import scala.collection.JavaConverters._
        rec.put(0, thing.asJava)
      }

      def decode(rec: GenericRecord): Map[String, Int] = ???
    }

    // We don't currently support writing TileFeature-based layers in RF.
    val writer = LayerWriter(tfLayer.base)
    val tlfRdd = ContextRDD(tfRdd, testRdd.metadata)
    writer.write(tfLayer.id, tlfRdd, ZCurveKeyIndexMethod.byDay())

    //TestData.sampleTileLayerRDD.toLayer.write.geotrellis.asLayer(sampleImageLayer).save()
    val writer2 = LayerWriter(sampleImageLayer.base)
    val imgRDD = TestData.sampleTileLayerRDD
    writer2.write(sampleImageLayer.id, imgRDD, ZCurveKeyIndexMethod)
  }

  describe("DataSource reading") {
    def layerReader = spark.read.geotrellis
    it("should read tiles") {
      val df = layerReader.loadLayer(layer)
      assert(df.count === tileCoordRange.length * tileCoordRange.length)
    }

    it("used produce tile UDT that we can manipulate") {
      val df = layerReader.loadLayer(layer)
        .select(SPATIAL_KEY_COLUMN, rf_tile_stats(TILE_COLUMN))
      assert(df.count() > 0)
    }

    it("should respect bbox query") {
      val boundKeys = KeyBounds(SpatialKey(3, 4), SpatialKey(4, 4))
      val  bbox = testRdd.metadata.layout
        .mapTransform(boundKeys.toGridBounds())
        .toPolygon()
      val wc = layerReader.loadLayer(layer).withCenter()

      withClue("literate API") {
        val df = wc.where(CENTER_COLUMN intersects bbox)
        assert(df.count() === boundKeys.toGridBounds.size)
      }
      withClue("functional API") {
        val df = wc.where(st_intersects(CENTER_COLUMN, geomLit(bbox)))
        assert(df.count() === boundKeys.toGridBounds.size)
      }
    }

    it("should invoke Encoder[Extent]") {
      val df = layerReader.loadLayer(layer).withGeometry()
      assert(df.count > 0)
      assert(df.first.length === 5)
      assert(df.first.getAs[Extent](2) !== null)
    }

    it("should write to parquet") {
      //just should not throw
      val df = layerReader.loadLayer(layer)
      assert(write(df))
    }
  }

  describe("DataSource options") {
    it("should respect partitions 2") {
      val expected = 2
      val df = spark.read.geotrellis
        .withNumPartitions(expected)
        .loadLayer(layer)
      assert(df.rdd.partitions.length === expected)
    }
    it("should respect partitions 20") {
      val expected = 20
      val df = spark.read.geotrellis
        .withNumPartitions(expected)
        .loadLayer(layer)
      assert(df.rdd.partitions.length === expected)
    }
    it("should respect subdivide 2") {
      val param = 2
      val df: RasterFrameLayer = spark.read.geotrellis
        .withTileSubdivisions(param)
        .loadLayer(layer)

      val dims = df.select(rf_dimensions(df.tileColumns.head)("cols"), rf_dimensions(df.tileColumns.head)("rows")).first()
      assert(dims.getAs[Int](0) === tileSize / param)
      assert(dims.getAs[Int](1) === tileSize / param)

      // row count will increase
      assert(df.count === testRdd.count() * param * param)
    }
    it("should respect subdivide with TileFeature"){
      val param = 2
      val rf: RasterFrameLayer = spark.read.geotrellis
        .withTileSubdivisions(param)
        .loadLayer(tfLayer)

      val dims = rf.select(rf_dimensions(rf.tileColumns.head)("cols"), rf_dimensions(rf.tileColumns.head)("rows"))
        .first()
      assert(dims.getAs[Int](0) === tileSize / param)
      assert(dims.getAs[Int](1) === tileSize / param)

      assert(rf.count() === testRdd.count() * param * param)
    }

    it("should respect both subdivideTile and numPartitions"){
      val subParam = 3

      val rf = spark.read
        .geotrellis
        .withNumPartitions(7)
        .withTileSubdivisions(subParam)
        .loadLayer(layer)

      // is it partitioned correctly?
      assert(rf.rdd.partitions.length === 7)

      // is it subdivided?
      assert(rf.count === testRdd.count * subParam * subParam)
      val dims = rf.select(rf_dimensions(rf.tileColumns.head)("cols"), rf_dimensions(rf.tileColumns.head)("rows"))
        .first()
      assert(dims.getAs[Int](0) === tileSize / subParam)
      assert(dims.getAs[Int](1) === tileSize / subParam)
    }

    it("should subdivide tiles properly") {
      val subs = 4
      val rf = spark.read.geotrellis
        .withTileSubdivisions(subs)
        .loadLayer(sampleImageLayer)


      assert(rf.count === (TestData.sampleTileLayerRDD.count * subs * subs))

      val Dimensions(width, height) = sampleGeoTiff.tile.dimensions

      val raster = rf.toRaster(rf.tileColumns.head, width, height, NearestNeighbor)

      assertEqual(raster.tile, sampleGeoTiff.tile)

      //GeoTiff(raster).write("target/from-split.tiff")
      // 774 x 500
    }

    it("should throw on subdivide 5") {
      // only throws when an action is taken...
      assertThrows[IllegalArgumentException](spark.read.geotrellis.withTileSubdivisions(5).loadLayer(layer).cache)
    }
    it("should throw on subdivide 13") {
      assertThrows[IllegalArgumentException](spark.read.geotrellis.withTileSubdivisions(13).loadLayer(layer).cache)
    }
    it("should throw on subdivide -3") {
      assertThrows[IllegalArgumentException](spark.read.geotrellis.withTileSubdivisions(-3).loadLayer(layer).count)
    }
  }

  describe("Predicate push-down support") {
    def layerReader: GeoTrellisRasterFrameReader = spark.read.geotrellis

    def extractRelation(df: DataFrame): Option[GeoTrellisRelation] = {
      val plan = df.queryExecution.optimizedPlan
      plan.collectFirst { case SpatialRelationReceiver(gt: GeoTrellisRelation) => gt }
    }

    def numFilters(df: DataFrame): Int =
      extractRelation(df).map(_.filters.length).getOrElse(0)

    def numSplitFilters(df: DataFrame): Int =
      extractRelation(df).map(r => splitFilters(r.filters).length).getOrElse(0)

    val pt1 = Point(-88, 60)
    val pt2 = Point(-78, 38)
    val region = Extent(
      min(pt1.x, pt2.x), max(pt1.x, pt2.x),
      min(pt1.y, pt2.y), max(pt1.y, pt2.y)
    )

    lazy val targetKey = testRdd.metadata.mapTransform(pt1)

    it("should support extent against a geometry literal") {
      val df: DataFrame = layerReader
        .loadLayer(layer)
        .where(GEOMETRY_COLUMN intersects pt1)

      // TODO: implement SpatialFilterPushdownRules
      // assert(numFilters(df) === 1)

      assert(df.count() === 1)
      assert(df.select(SPATIAL_KEY_COLUMN).first === targetKey)
    }

    it("should support query with multiple geometry types") {
      // Mostly just testing that these evaluate without catalyst type errors.
      forEvery(GeomData.all) { g =>
        val query = layerReader.loadLayer(layer).where(GEOMETRY_COLUMN.intersects(g))
          .persist(StorageLevel.OFF_HEAP)
        assert(query.count() === 0)
      }
    }

    it("should *not* support extent filter against a UDF") {
      val targetKey = testRdd.metadata.mapTransform(pt1)

      val mkPtFcn = sparkUdf((_: Row) => { Point(-88, 60) })

      val df = layerReader
        .loadLayer(layer)
        .where(st_intersects(GEOMETRY_COLUMN, mkPtFcn(SPATIAL_KEY_COLUMN)))

      // TODO: implement SpatialFilterPushdownRules
      assert(numFilters(df) === 0)

      assert(df.count() === 1)
      assert(df.select(SPATIAL_KEY_COLUMN).first === targetKey)
    }

    ignore("should support temporal predicates") {
      withClue("at now") {
        val df = layerReader
          .loadLayer(layer)
          .where(TIMESTAMP_COLUMN === Timestamp.valueOf(now.toLocalDateTime))

        // TODO: implement SpatialFilterPushdownRules
        // assert(numFilters(df) == 1)
        assert(df.count() == testRdd.count())
      }

      withClue("at earlier") {
        val df = layerReader
          .loadLayer(layer)
          .where(TIMESTAMP_COLUMN === Timestamp.valueOf(now.minusDays(1).toLocalDateTime))

        // TODO: implement SpatialFilterPushdownRules
        // assert(numFilters(df) === 1)
        assert(df.count() == 0)
      }

      withClue("between now") {
        val df = layerReader
          .loadLayer(layer)
          .where(TIMESTAMP_COLUMN betweenTimes (now.minusDays(1), now.plusDays(1)))

        // TODO: implement SpatialFilterPushdownRules
        // assert(numFilters(df) === 1)
        assert(df.count() == testRdd.count())
      }

      withClue("between later") {
        val df = layerReader
          .loadLayer(layer)
          .where(TIMESTAMP_COLUMN betweenTimes (now.plusDays(1), now.plusDays(2)))

        // TODO: implement SpatialFilterPushdownRules
        // assert(numFilters(df) === 1)
        assert(df.count() == 0)
      }
    }

    ignore("should support nested predicates") {
      withClue("fully nested") {
        val df = layerReader
          .loadLayer(layer)
          .where(
            ((GEOMETRY_COLUMN intersects pt1) ||
              (GEOMETRY_COLUMN intersects pt2)) &&
              (TIMESTAMP_COLUMN === Timestamp.valueOf(now.toLocalDateTime))
          )

        // TODO: implement SpatialFilterPushdownRules
        // assert(numFilters(df) === 1)
        // assert(numSplitFilters(df) === 2, extractRelation(df).toString)

        assert(df.count === 2)
      }

      withClue("partially nested") {
        val df = layerReader
          .loadLayer(layer)
          .where((GEOMETRY_COLUMN intersects pt1) || (GEOMETRY_COLUMN intersects pt2))
          .where(TIMESTAMP_COLUMN === Timestamp.valueOf(now.toLocalDateTime))

        // TODO: implement SpatialFilterPushdownRules
        // assert(numFilters(df) === 1)
        // assert(numSplitFilters(df) === 2, extractRelation(df).toString)

        assert(df.count === 2)
      }
    }

    it("should support intersects with between times") {
      withClue("intersects first") {
        val df = layerReader
          .loadLayer(layer)
          .where(GEOMETRY_COLUMN intersects pt1)
          .where(TIMESTAMP_COLUMN betweenTimes(now.minusDays(1), now.plusDays(1)))

        // TODO: implement SpatialFilterPushdownRules
        // assert(numFilters(df) == 1)
      }
      withClue("intersects last") {
        val df = layerReader
          .loadLayer(layer)
          .where(TIMESTAMP_COLUMN betweenTimes(now.minusDays(1), now.plusDays(1)))
          .where(GEOMETRY_COLUMN intersects pt1)

        // TODO: implement SpatialFilterPushdownRules
        // assert(numFilters(df) == 1)
      }

      withClue("untyped columns") {
        import spark.implicits._
        val df = layerReader
          .loadLayer(layer)
          .where($"timestamp" >= Timestamp.valueOf(now.minusDays(1).toLocalDateTime))
          .where($"timestamp" <= Timestamp.valueOf(now.plusDays(1).toLocalDateTime))
          .where(st_intersects(GEOMETRY_COLUMN, geomLit(pt1)))

        // TODO: implement SpatialFilterPushdownRules
        // assert(numFilters(df) == 1)
      }

    }

    it("should handle renamed spatial filter columns") {
      val df = layerReader
        .loadLayer(layer)
        .where(GEOMETRY_COLUMN intersects region)
        .withColumnRenamed(GEOMETRY_COLUMN.columnName, "foobar")

      // TODO: implement SpatialFilterPushdownRules
      // assert(numFilters(df) === 1)
      assert(df.count > 0, df.schema.treeString)
    }

    it("should handle dropped spatial filter columns") {
      val df = layerReader
        .loadLayer(layer)
        .where(GEOMETRY_COLUMN intersects region)
        .drop(GEOMETRY_COLUMN)

      // TODO: implement SpatialFilterPushdownRules
      // assert(numFilters(df) === 1)
    }
  }

  describe("TileFeature support") {
    def layerReader = spark.read.geotrellis
    it("should resolve TileFeature-based RasterFrameLayer") {
      val rf = layerReader.loadLayer(tfLayer)
      //rf.show(false)
      assert(rf.collect().length === testRdd.count())
    }
    it("should respect subdivideTile option on TileFeature RasterFrameLayer") {
      val subParam = 4
      val rf = spark.read.option(TILE_SUBDIVISIONS_PARAM, subParam).geotrellis.loadLayer(tfLayer)

      assert(rf.count === testRdd.count * subParam * subParam)

      val dims = rf.select(rf_dimensions(rf.tileColumns.head)("cols"), rf_dimensions(rf.tileColumns.head)("rows"))
        .first()
      assert(dims.getAs[Int](0) === tileSize / subParam)
      assert(dims.getAs[Int](1) === tileSize / subParam)
    }
    it("should respect both `subdivideTile` and `numPartition` options on TileFeature"){
      val subParam = 2

      val rf = spark.read
        .option(TILE_SUBDIVISIONS_PARAM, subParam)
        .option(NUM_PARTITIONS_PARAM, 10)
        .geotrellis.loadLayer(tfLayer)

      // is it subdivided?
      assert(rf.count === testRdd.count * subParam * subParam)
      val dims = rf.select(rf_dimensions(rf.tileColumns.head)("cols"), rf_dimensions(rf.tileColumns.head)("rows"))
        .first()
      assert(dims.getAs[Int](0) === tileSize / subParam)
      assert(dims.getAs[Int](1) === tileSize / subParam)

      // is it partitioned correctly?
      assert(rf.rdd.partitions.length === 10)
    }
    it("should respect options on spatial-only TileFeature"){
      assert(true === true)
    }
  }
}
