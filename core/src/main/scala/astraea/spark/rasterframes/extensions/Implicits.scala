/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Astraea, Inc.
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

package astraea.spark.rasterframes.extensions

import astraea.spark.rasterframes.{PairRDDConverter, RasterFrame}
import astraea.spark.rasterframes.util.{WithMergeMethods, WithPrototypeMethods}
import geotrellis.raster._
import geotrellis.spark.{Metadata, SpaceTimeKey, SpatialComponent, SpatialKey, TileLayerMetadata}
import geotrellis.util.MethodExtensions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{MetadataBuilder, Metadata ⇒ SMetadata}
import spray.json.JsonFormat

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Library-wide implicit class definitions.
 *
 * @since 12/21/17
 */
trait Implicits {
  implicit class WithSparkSessionMethods(val self: SparkSession) extends SparkSessionMethods

  implicit class WithSQLContextMethods(val self: SQLContext) extends SQLContextMethods

  implicit class WithProjectedRasterMethods[T <: CellGrid: WithMergeMethods: WithPrototypeMethods: TypeTag](
    val self: ProjectedRaster[T]) extends ProjectedRasterMethods[T]

  implicit class WithDataFrameMethods[D <: DataFrame](val self: D) extends DataFrameMethods[D]

  implicit class WithRasterFrameMethods(val self: RasterFrame) extends RasterFrameMethods

  implicit class WithSpatialContextRDDMethods[T <: CellGrid](
    val self: RDD[(SpatialKey, T)] with Metadata[TileLayerMetadata[SpatialKey]]
  )(implicit spark: SparkSession) extends SpatialContextRDDMethods[T]

  implicit class WithSpatioTemporalContextRDDMethods[T <: CellGrid](
    val self: RDD[(SpaceTimeKey, T)] with Metadata[TileLayerMetadata[SpaceTimeKey]]
  )(implicit spark: SparkSession) extends SpatioTemporalContextRDDMethods[T]

  private[astraea] implicit class WithMetadataMethods[R: JsonFormat](val self: R)
      extends MetadataMethods[R]

  private[astraea] implicit class WithMetadataAppendMethods(val self: SMetadata)
      extends MethodExtensions[SMetadata] {
    def append = new MetadataBuilder().withMetadata(self)
  }

  private[astraea] implicit class WithMetadataBuilderMethods(val self: MetadataBuilder)
      extends MetadataBuilderMethods
}

object Implicits extends Implicits

