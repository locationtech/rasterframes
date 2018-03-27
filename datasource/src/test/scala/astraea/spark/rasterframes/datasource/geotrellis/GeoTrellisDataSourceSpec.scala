/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017-2018 Azavea & Astraea, Inc.
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
package astraea.spark.rasterframes.datasource.geotrellis

import java.io.File
import java.time.ZonedDateTime

import astraea.spark.rasterframes._
import astraea.spark.rasterframes.util._
import geotrellis.proj4.LatLng
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.avro.AvroRecordCodec
import geotrellis.spark.io.avro.codecs.Implicits._
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.tiling.ZoomedLayoutScheme
import geotrellis.vector._
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.hadoop.fs.FileUtil
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.functions.{udf ⇒ sparkUdf, _}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{BeforeAndAfter, Inspectors}
import org.apache.avro.generic._
import org.apache.spark.storage.StorageLevel

import scala.math.{min, max}

class GeoTrellisDataSourceSpec
    extends TestEnvironment with TestData with BeforeAndAfter with Inspectors
    with IntelliJPresentationCompilerHack {

  lazy val layer = Layer(new File(outputLocalPath).toURI, LayerId("test-layer", 4))
  lazy val tfLayer = Layer(new File(outputLocalPath).toURI, LayerId("test-tf-layer", 4))
  val now = ZonedDateTime.now()
  val tileCoordRange = 2 to 5

  lazy val testRdd = {
    val recs: Seq[(SpaceTimeKey, Tile)] = for {
      col ← tileCoordRange
      row ← tileCoordRange
    } yield SpaceTimeKey(col, row, now) -> ArrayTile.alloc(DoubleConstantNoDataCellType, 12, 12)

    val rdd = sc.parallelize(recs)
    val scheme = ZoomedLayoutScheme(LatLng, tileSize = 12)
    val layerLayout = scheme.levelForZoom(4).layout
    val layerBounds = KeyBounds(SpaceTimeKey(2, 2, now.minusMonths(1)), SpaceTimeKey(5, 5, now.plusMonths(1)))
    val md = TileLayerMetadata[SpaceTimeKey](
      cellType = DoubleConstantNoDataCellType,
      crs = LatLng,
      bounds = layerBounds,
      layout = layerLayout,
      extent = layerLayout.mapTransform(layerBounds.toGridBounds()))
    ContextRDD(rdd, md)
  }

  before {
    val outputDir = new File(layer.base)
    FileUtil.fullyDelete(outputDir)
    outputDir.deleteOnExit()

    // Test layer writing via RF
    testRdd.toRF.write.geotrellis.asLayer(layer).save()

    val tfRdd = testRdd.map { case (k, tile) ⇒
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
  }


  describe("DataSource reading") {
    def layerReader = spark.read.geotrellis
    it("should read tiles") {
      val df = layerReader.loadRF(layer)
      assert(df.count === tileCoordRange.length * tileCoordRange.length)
    }

    it("used produce tile UDT that we can manipulate") {
      val df = layerReader.loadRF(layer)
        .select(SPATIAL_KEY_COLUMN, tileStats(TILE_COLUMN))
      assert(df.count() > 0)
    }

    it("should respect bbox query") {
      val boundKeys = KeyBounds(SpatialKey(3, 4), SpatialKey(4, 4))
      val  bbox = testRdd.metadata.layout
        .mapTransform(boundKeys.toGridBounds())
        .jtsGeom
      val wc = layerReader.loadRF(layer).withCenter()

      withClue("literate API") {
        val df = wc.where(CENTER_COLUMN intersects bbox)
        assert(df.count() === boundKeys.toGridBounds.sizeLong)
      }
      withClue("functional API") {
        val df = wc.where(st_intersects(CENTER_COLUMN, geomLit(bbox)))
        assert(df.count() === boundKeys.toGridBounds.sizeLong)
      }
    }

    it("should invoke Encoder[Extent]") {
      val df = layerReader.loadRF(layer).withBounds()
      assert(df.count > 0)
      assert(df.first.length === 5)
      assert(df.first.getAs[Extent](2) !== null)
    }

    it("should write to parquet") {
      //just should not throw
      val df = layerReader.loadRF(layer)
      assert(write(df))
    }
  }

  describe("DataSource options") {
    def layerReader = spark.read.geotrellis

    it("should respect partitions 2") {
      val expected = 2
      val df = spark.read.option("numPartitions", expected)
          .geotrellis.loadRF(layer)
      assert(df.rdd.partitions.length === expected)
    }
    it("should respect partitions 20") {
      val expected = 20
      val df = spark.read.option("numPartitions", expected)
        .geotrellis.loadRF(layer)
      assert(df.rdd.partitions.length === expected)
    }
  }

  describe("Predicate push-down support") {
    def layerReader = spark.read.geotrellis

    def extractRelation(df: DataFrame): Option[GeoTrellisRelation] = {
      val plan = df.queryExecution.optimizedPlan
      plan.collectFirst {
        case LogicalRelation(gt: GeoTrellisRelation, _, _) ⇒ gt
      }
    }
    def numFilters(df: DataFrame) = {
      extractRelation(df).map(_.filters.length).getOrElse(0)
    }
    def numSplitFilters(df: DataFrame) = {
      extractRelation(df).map(_.splitFilters.length).getOrElse(0)
    }

    val pt1 = Point(-88, 60)
    val pt2 = Point(-78, 38)
    val region = Extent(
      min(pt1.x, pt2.x), max(pt1.x, pt2.x),
      min(pt1.y, pt2.y), max(pt1.y, pt2.y)
    )

    val targetKey = testRdd.metadata.mapTransform(pt1)

    it("should support extent against a geometry literal") {
      val df: DataFrame = layerReader
        .loadRF(layer)
        .where(BOUNDS_COLUMN intersects pt1)

      assert(numFilters(df) === 1)

      assert(df.count() === 1)
      assert(df.select(SPATIAL_KEY_COLUMN).first === targetKey)
    }

    it("should support query with multiple geometry types") {
      // Mostly just testing that these evaluate without catalyst type errors.
      forEvery(JTS.all) { g ⇒
        val query = layerReader.loadRF(layer).where(BOUNDS_COLUMN.intersects(g))
          .persist(StorageLevel.OFF_HEAP)
        assert(query.count() === 0)
      }
    }

    it("should *not* support extent filter against a UDF") {
      val targetKey = testRdd.metadata.mapTransform(pt1)

      val mkPtFcn = sparkUdf((_: Row) ⇒ { Point(-88, 60).jtsGeom })

      val df = layerReader
        .loadRF(layer)
        .where(st_intersects(BOUNDS_COLUMN, mkPtFcn(SPATIAL_KEY_COLUMN)))

      assert(numFilters(df) === 0)

      assert(df.count() === 1)
      assert(df.select(SPATIAL_KEY_COLUMN).first === targetKey)
    }

    it("should support temporal predicates") {
      withClue("at now") {
        val df = layerReader
          .loadRF(layer)
          .where(TIMESTAMP_COLUMN at now)

        assert(numFilters(df) == 1)
        assert(df.count() == testRdd.count())
      }

      withClue("at earlier") {
        val df = layerReader
          .loadRF(layer)
          .where(TIMESTAMP_COLUMN at now.minusDays(1))

        assert(numFilters(df) === 1)
        assert(df.count() == 0)
      }

      withClue("between now") {
        val df = layerReader
          .loadRF(layer)
          .where(TIMESTAMP_COLUMN betweenTimes (now.minusDays(1), now.plusDays(1)))

        assert(numFilters(df) === 1)
        assert(df.count() == testRdd.count())
      }

      withClue("between later") {
        val df = layerReader
          .loadRF(layer)
          .where(TIMESTAMP_COLUMN betweenTimes (now.plusDays(1), now.plusDays(2)))

        assert(numFilters(df) === 1)
        assert(df.count() == 0)
      }
    }

    it("should support nested predicates") {
      withClue("fully nested") {
        val df = layerReader
          .loadRF(layer)
          .where(
            ((BOUNDS_COLUMN intersects pt1) ||
              (BOUNDS_COLUMN intersects pt2)) &&
              (TIMESTAMP_COLUMN at now)
          )

        assert(numFilters(df) === 1)
        assert(numSplitFilters(df) === 2, extractRelation(df).toString)

        assert(df.count === 2)
      }

      withClue("partially nested") {
        val df = layerReader
          .loadRF(layer)
          .where((BOUNDS_COLUMN intersects pt1) || (BOUNDS_COLUMN intersects pt2))
          .where(TIMESTAMP_COLUMN at now)

        assert(numFilters(df) === 1)
        assert(numSplitFilters(df) === 2, extractRelation(df).toString)

        assert(df.count === 2)
      }
    }

    it("should support intersects with between times") {
      val df = layerReader
        .loadRF(layer)
        .where(BOUNDS_COLUMN intersects pt1)
        .where(TIMESTAMP_COLUMN betweenTimes(now.minusDays(1), now.plusDays(1)))

      assert(numFilters(df) == 1)
    }

    it("should handle renamed spatial filter columns") {
      val df = layerReader
        .loadRF(layer)
        .where(BOUNDS_COLUMN intersects region.jtsGeom)
        .withColumnRenamed(BOUNDS_COLUMN.columnName, "foobar")

      assert(numFilters(df) === 1, df.explain(true))
      assert(df.count > 0, df.printSchema)
    }

    it("should handle dropped spatial filter columns") {
      val df = layerReader
        .loadRF(layer)
        .where(BOUNDS_COLUMN intersects region.jtsGeom)
        .drop(BOUNDS_COLUMN)

      assert(numFilters(df) === 1, df.explain(true))
    }
  }

  describe("TileFeature support") {
    def layerReader = spark.read.geotrellis
    it("should resolve TileFeature-based RasterFrame") {
      val rf = layerReader.loadRF(tfLayer)
      //rf.show(false)
      assert(rf.collect().length === testRdd.count())
    }
  }
}
