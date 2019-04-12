/*
 * Copyright 2017 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package astraea.spark

import astraea.spark.rasterframes.encoders.StandardEncoders
import astraea.spark.rasterframes.model.TileDimensions
import astraea.spark.rasterframes.util.ZeroSevenCompatibilityKit
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.{Tile, TileFeature}
import geotrellis.spark.{ContextRDD, Metadata, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.locationtech.geomesa.spark.jts.DataFrameFunctions
import shapeless.tag.@@

import scala.reflect.runtime.universe._

/**
 *  Module providing support for RasterFrames.
 * `import astraea.spark.rasterframes._`.
 *
 * @since 7/18/17
 */
package object rasterframes extends StandardColumns
  with RasterFunctions
  with ZeroSevenCompatibilityKit.RasterFunctions
  with rasterframes.extensions.Implicits
  with rasterframes.jts.Implicits
  with StandardEncoders
  with DataFrameFunctions.Library
  with LazyLogging {

  @transient
  private[rasterframes]
  val rfConfig = ConfigFactory.load().getConfig("rasterframes")

  /** The generally expected tile size, as defined by configuration property `rasterframes.nominal-tile-size`.*/
  @transient
  final val NOMINAL_TILE_SIZE: Int = rfConfig.getInt("nominal-tile-size")
  final val NOMINAL_TILE_DIMS: TileDimensions = TileDimensions(NOMINAL_TILE_SIZE, NOMINAL_TILE_SIZE)

  /**
   * Initialization injection point. Must be called before any RasterFrame
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
    ZeroSevenCompatibilityKit.register(sqlContext)
    rasterframes.functions.register(sqlContext)
    rasterframes.expressions.register(sqlContext)
    rasterframes.rules.register(sqlContext)
  }

  /**
   * A RasterFrame is just a DataFrame with certain invariants, enforced via the methods that create and transform them:
   *   1. One column is a [[geotrellis.spark.SpatialKey]] or [[geotrellis.spark.SpaceTimeKey]]
   *   2. One or more columns is a [[Tile]] UDT.
   *   3. The `TileLayerMetadata` is encoded and attached to the key column.
   */
  type RasterFrame = DataFrame @@ RasterFrameTag

  /** Tagged type for allowing compiler to help keep track of what has RasterFrame assurances applied to it. */
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
}
