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
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.locationtech
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import geotrellis.raster.{Dimensions, Tile, TileFeature, isData}
import geotrellis.raster.resample._
import geotrellis.layer._
import geotrellis.spark.ContextRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.rf.{RasterSourceUDT, TileUDT}
import org.apache.spark.sql.{DataFrame, SQLContext, rf}
import org.locationtech.geomesa.spark.jts.DataFrameFunctions
import org.locationtech.rasterframes.encoders.StandardEncoders
import org.locationtech.rasterframes.extensions.Implicits
import org.slf4j.LoggerFactory
import shapeless.tag.@@

import scala.reflect.runtime.universe._

package object rasterframes extends StandardColumns
  with RasterFunctions
  with Implicits
  with rasterframes.jts.Implicits
  with StandardEncoders
  with DataFrameFunctions.Library {

  // Don't make this a `lazy val`... breaks Spark assemblies for some reason.
  protected def logger: Logger = Logger(LoggerFactory.getLogger(getClass.getName))

  private[rasterframes]
  def rfConfig = ConfigFactory.load().getConfig("rasterframes")

  /** The generally expected tile size, as defined by configuration property `rasterframes.nominal-tile-size`.*/
  @transient
  final val NOMINAL_TILE_SIZE: Int = rfConfig.getInt("nominal-tile-size")
  final val NOMINAL_TILE_DIMS: Dimensions[Int] = Dimensions(NOMINAL_TILE_SIZE, NOMINAL_TILE_SIZE)

  /**
   * Initialization injection point. Must be called before any RasterFrameLayer
   * types are used.
   */
  def initRF(sqlContext: SQLContext): Unit = {
    import org.locationtech.geomesa.spark.jts._
    sqlContext.withJTS

    val config = sqlContext.sparkSession.conf
    if(config.getOption("spark.serializer").isEmpty) {
      logger.warn("No serializer has been registered with Spark. Default Java serialization will be used, which is slow. " +
        "Consider using the following settings:" +
        """
          |    SparkSession
          |        .builder()
          |        .master("...")
          |        .appName("...")
          |        .withKryoSerialization  // <--- RasterFrames extension method
      """.stripMargin

      )
    }

    rf.register(sqlContext)
    rasterframes.functions.register(sqlContext)
    rasterframes.expressions.register(sqlContext)
    rasterframes.rules.register(sqlContext)
  }

  /** TileUDT type reference. */
  def TileType = new TileUDT()

  /** RasterSourceUDT type reference. */
  def RasterSourceType = new RasterSourceUDT()

  /**
   * A RasterFrameLayer is just a DataFrame with certain invariants, enforced via the methods that create and transform them:
   *   1. One column is a `SpatialKey` or `SpaceTimeKey``
   *   2. One or more columns is a [[Tile]] UDT.
   *   3. The `TileLayerMetadata` is encoded and attached to the key column.
   */
  type RasterFrameLayer = DataFrame @@ RasterFrameTag

  /** Tagged type for allowing compiler to help keep track of what has RasterFrameLayer assurances applied to it. */
  trait RasterFrameTag

  type TileFeatureLayerRDD[K, D] =
    RDD[(K, TileFeature[Tile, D])] with Metadata[TileLayerMetadata[K]]

  object TileFeatureLayerRDD {
    def apply[K, D](rdd: RDD[(K, TileFeature[Tile, D])],
      metadata: TileLayerMetadata[K]): TileFeatureLayerRDD[K, D] =
      new ContextRDD(rdd, metadata)
  }

  /** Provides evidence that a given primitive has an associated CellType. */
  trait HasCellType[T] extends Serializable
  object HasCellType {
    implicit val intHasCellType = new HasCellType[Int] {}
    implicit val doubleHasCellType = new HasCellType[Double] {}
    implicit val byteHasCellType = new HasCellType[Byte] {}
    implicit val shortHasCellType = new HasCellType[Short] {}
    implicit val floatHasCellType = new HasCellType[Float] {}
  }

  /** Evidence type class for communicating that only standard key types are supported with the more general GeoTrellis type parameters. */
  trait StandardLayerKey[T] extends Serializable {
    val selfType: TypeTag[T]
    def isType[R: TypeTag]: Boolean = typeOf[R] =:= selfType.tpe
    def coerce[K >: T](tlm: TileLayerMetadata[_]): TileLayerMetadata[K] =
      tlm.asInstanceOf[TileLayerMetadata[K]]
  }
  object StandardLayerKey {
    def apply[T: StandardLayerKey]: StandardLayerKey[T] = implicitly
    implicit val spatialKeySupport = new StandardLayerKey[SpatialKey] {
      override val selfType: TypeTag[SpatialKey] = implicitly
    }
    implicit val spatioTemporalKeySupport = new StandardLayerKey[SpaceTimeKey] {
      override val selfType: TypeTag[SpaceTimeKey] = implicitly
    }
  }

  /** Test if a cell value evaluates to true: it is not NoData and it is non-zero */
  def isCellTrue(v: Double): Boolean =  isData(v) & v != 0.0
  /** Test if a cell value evaluates to true: it is not NoData and it is non-zero */
  def isCellTrue(v: Int): Boolean =  isData(v) & v != 0

  /** Test if a Tile's cell value evaluates to true at a given position. Truth defined by not NoData and non-zero */
  def isCellTrue(t: Tile, col: Int, row: Int): Boolean =
    if (t.cellType.isFloatingPoint) isCellTrue(t.getDouble(col, row))
    else isCellTrue(t.get(col, row))





}
