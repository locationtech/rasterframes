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

import org.locationtech.rasterframes.util._
import geotrellis.raster.{MultibandTile, Tile, TileFeature}
import geotrellis.spark.{SpaceTimeKey, SpatialKey}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.rf.TileUDT
import org.apache.spark.sql.types._

import scala.annotation.implicitNotFound

/**
 * Typeclass for converting a Pair RDD into a dataframe.
 *
 * @since 4/8/18
 */
@implicitNotFound("An RDD converter is required create a RasterFrameLayer. " +
  "Please provide an implementation of PairRDDConverter[${K}, ${V}].")
trait PairRDDConverter[K, V] extends Serializable {
  val schema: StructType
  def toDataFrame(rdd: RDD[(K, V)])(implicit spark: SparkSession): DataFrame
}

object PairRDDConverter {
  /** Enrichment over a pair RDD for converting it to a DataFrame given a converter. */
  implicit class RDDCanBeDataFrame[K, V](rdd: RDD[(K, V)])(implicit spark: SparkSession, converter: PairRDDConverter[K, V]) {
    def toDataFrame: DataFrame = converter.toDataFrame(rdd)
  }

  // Hack around Spark bug when singletons are used in schemas
  private val serializableTileUDT = new TileUDT()

  /** Fetch converter from implicit scope. */
  def apply[K, V](implicit sp: PairRDDConverter[K, V]) = sp

  /** Enables conversion of `RDD[(SpatialKey, Tile)]` to DataFrame. */
  implicit val spatialTileConverter = new PairRDDConverter[SpatialKey, Tile] {
    val schema: StructType = {
      StructType(Seq(
        StructField(SPATIAL_KEY_COLUMN.columnName, spatialKeyEncoder.schema, nullable = false),
        StructField(TILE_COLUMN.columnName, serializableTileUDT, nullable = false)
      ))
    }

    def toDataFrame(rdd: RDD[(SpatialKey, Tile)])(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._
      rdd.toDF(schema.fields.map(_.name): _*)
    }
  }

  /** Enables conversion of `RDD[(SpaceTimeKey, Tile)]` to DataFrame. */
  implicit val spaceTimeTileConverter = new PairRDDConverter[SpaceTimeKey, Tile] {
    val schema: StructType = {
      val base = spatialTileConverter.schema
      val addedFields = Seq(StructField(TEMPORAL_KEY_COLUMN.columnName, temporalKeyEncoder.schema, nullable = false))
      StructType(base.fields.patch(1, addedFields, 0))
    }

    def toDataFrame(rdd: RDD[(SpaceTimeKey, Tile)])(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._
      rdd.map{ case (k, v) ⇒ (k.spatialKey, k.temporalKey, v)}.toDF(schema.fields.map(_.name): _*)
    }
  }

  /** Enables conversion of `RDD[(SpatialKey, TileFeature[Tile, D])]` to DataFrame. */
  implicit def spatialTileFeatureConverter[D: Encoder] = new PairRDDConverter[SpatialKey, TileFeature[Tile, D]] {
    implicit val featureEncoder = implicitly[Encoder[D]]
    implicit val rowEncoder = Encoders.tuple(spatialKeyEncoder, singlebandTileEncoder, featureEncoder)

    val schema: StructType = {
      val base = spatialTileConverter.schema
      StructType(base.fields :+ StructField(TILE_FEATURE_DATA_COLUMN.columnName, featureEncoder.schema, nullable = true))
    }

    def toDataFrame(rdd: RDD[(SpatialKey, TileFeature[Tile, D])])(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._
      rdd.map{ case (k, v) ⇒ (k, v.tile, v.data)}.toDF(schema.fields.map(_.name): _*)
    }
  }

  /** Enables conversion of `RDD[(SpaceTimeKey, TileFeature[Tile, D])]` to DataFrame. */
  implicit def spaceTimeTileFeatureConverter[D: Encoder] = new PairRDDConverter[SpaceTimeKey, TileFeature[Tile, D]] {
    implicit val featureEncoder = implicitly[Encoder[D]]
    implicit val rowEncoder = Encoders.tuple(spatialKeyEncoder, temporalKeyEncoder, singlebandTileEncoder, featureEncoder)

    val schema: StructType = {
      val base = spaceTimeTileConverter.schema
      StructType(base.fields :+ StructField(TILE_FEATURE_DATA_COLUMN.columnName, featureEncoder.schema, nullable = true))
    }

    def toDataFrame(rdd: RDD[(SpaceTimeKey, TileFeature[Tile, D])])(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._
      val tupRDD = rdd.map { case (k, v) ⇒ (k.spatialKey, k.temporalKey, v.tile, v.data) }

      rddToDatasetHolder(tupRDD)
      tupRDD.toDF(schema.fields.map(_.name): _*)
    }
  }

  /** Enables conversion of `RDD[(SpatialKey, MultibandTile)]` to DataFrame. */
  def forSpatialMultiband(bands: Int) = new PairRDDConverter[SpatialKey, MultibandTile] {
    val schema: StructType = {
      val base = spatialTileConverter.schema

      val basename = TILE_COLUMN.columnName

      val tiles = for(i ← 1 to bands) yield {
        val name = if(bands <= 1) basename else s"${basename}_$i"
        StructField(name , serializableTileUDT, nullable = false)
      }

      StructType(base.fields.patch(1, tiles, 1))
    }

    def toDataFrame(rdd: RDD[(SpatialKey, MultibandTile)])(implicit spark: SparkSession): DataFrame = {
      spark.createDataFrame(
        rdd.map { case (k, v) ⇒ Row(Row(k.col, k.row) +: v.bands: _*) },
        schema
      )
    }
  }

  /** Enables conversion of `RDD[(SpaceTimeKey, MultibandTile)]` to DataFrame. */
  def forSpaceTimeMultiband(bands: Int) = new PairRDDConverter[SpaceTimeKey, MultibandTile] {
    val schema: StructType = {
      val base = spaceTimeTileConverter.schema

      val basename = TILE_COLUMN.columnName

      val tiles = for(i ← 1 to bands) yield {
        StructField(s"${basename}_$i" , serializableTileUDT, nullable = false)
      }

      StructType(base.fields.patch(2, tiles, 1))
    }

    def toDataFrame(rdd: RDD[(SpaceTimeKey, MultibandTile)])(implicit spark: SparkSession): DataFrame = {
      spark.createDataFrame(
        rdd.map { case (k, v) ⇒ Row(Seq(Row(k.spatialKey.col, k.spatialKey.row), Row(k.temporalKey)) ++ v.bands: _*) },
        schema
      )
    }
  }
}
