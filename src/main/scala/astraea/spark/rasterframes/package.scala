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

import astraea.spark.rasterframes.encoders.EncoderImplicits
import geotrellis.raster.{ProjectedRaster, Tile, TileFeature}
import geotrellis.spark.{Bounds, ContextRDD, Metadata, SpaceTimeKey, SpatialComponent, TileLayerMetadata}
import geotrellis.util.GetComponent
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference}
import shapeless.tag.@@
import shapeless.{Lub, tag}
import spray.json.JsonFormat

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 *  Module providing support for RasterFrames.
 * `import astraea.spark.rasterframes._`., and then call `rfInit(SQLContext)`.
 *
 * @author sfitch
 * @since 7/18/17
 */
package object rasterframes extends EncoderImplicits with ColumnFunctions {
  type Statistics = astraea.spark.rasterframes.functions.CellStatsAggregateFunction.Statistics

  /**
   * Initialization injection point. Must be called before any RasterFrame
   * types are used.
   */
  @deprecated("Please use 'SparkSession.withRasterFrames' or 'SQLContext.withRasterFrames' instead.", "0.5.3")
  def rfInit(sqlContext: SQLContext): Unit = sqlContext.withRasterFrames

  /** Default RasterFrame spatial column name. */
  val SPATIAL_KEY_COLUMN = "spatial_key"

  /** Default RasterFrame temporal column name. */
  val TEMPORAL_KEY_COLUMN = "temporal_key"

  /** Default RasterFrame tile column name. */
  val TILE_COLUMN = "tile"

  /** Default RasterFrame [[TileFeature.data]] column name. */
  val TILE_FEATURE_DATA_COLUMN = "tile_data"

  /** Default teil column index column for the cells of exploded tiles. */
  val COLUMN_INDEX_COLUMN = "column_index"

  /** Default teil column index column for the cells of exploded tiles. */
  val ROW_INDEX_COLUMN = "row_index"

  /** Key under which ContextRDD metadata is stored. */
  private[rasterframes] val CONTEXT_METADATA_KEY = "_context"

  /** Key under which RasterFrame role a column plays. */
  private[rasterframes] val SPATIAL_ROLE_KEY = "_stRole"

  /**
   * A RasterFrame is just a DataFrame with certain invariants, enforced via the methods that create and transform them:
   *   1. One column is a [[geotrellis.spark.SpatialKey]] or [[geotrellis.spark.SpaceTimeKey]]
   *   2. One or more columns is a [[Tile]] UDT.
   *   3. The `TileLayerMetadata` is encoded and attached to the key column.
   */
  type RasterFrame = DataFrame @@ RasterFrameTag

  /** Tagged type for allowing compiler to help keep track of what has RasterFrame assurances applied to it. */
  trait RasterFrameTag

  /** Internal method for slapping the RasterFreame seal of approval on a DataFrame. */
  private[rasterframes] def certifyRasterframe(df: DataFrame): RasterFrame =
    tag[RasterFrameTag][DataFrame](df)

  /**
   * Type lambda alias for components that have bounds with parameterized key.
   * @tparam K bounds key type
   */
  type BoundsComponentOf[K] = {
    type get[M] = GetComponent[M, Bounds[K]]
  }

  trait HasCellType[T] extends Serializable
  object HasCellType {
    implicit val intHasCellType = new HasCellType[Int] {}
    implicit val doubleHasCellType = new HasCellType[Double] {}
    implicit val byteHasCellType = new HasCellType[Byte] {}
    implicit val shortHasCellType = new HasCellType[Short] {}
    implicit val floatHasCellType = new HasCellType[Float] {}
  }

  // ----------- Extension Method Injections: Beware the Ugly ------------

  implicit class WithSparkSessionMethods(val self: SparkSession)
    extends SparkSessionMethods

  implicit class WithSQLContextMethods(val self: SQLContext)
    extends SQLContextMethods

  implicit class WithProjectedRasterMethods(val self: ProjectedRaster[Tile])
      extends ProjectedRasterMethods

  implicit class WithDataFrameMethods(val self: DataFrame) extends DataFrameMethods

  implicit class WithRasterFrameMethods(val self: RasterFrame) extends RasterFrameMethods

  implicit class WithSpatialContextRDDMethods[K: SpatialComponent: JsonFormat: TypeTag](
    val self: RDD[(K, Tile)] with Metadata[TileLayerMetadata[K]]
  )(implicit spark: SparkSession)
      extends SpatialContextRDDMethods[K]

  implicit class WithSpatioTemporalContextRDDMethods(
    val self: RDD[(SpaceTimeKey, Tile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]
  )(implicit spark: SparkSession)
      extends SpatioTemporalContextRDDMethods

  implicit class WithTFContextRDDMethods[K: SpatialComponent: JsonFormat: ClassTag: TypeTag,
                                         D: TypeTag](
    val self: RDD[(K, TileFeature[Tile, D])] with Metadata[TileLayerMetadata[K]]
  )(implicit spark: SparkSession)
      extends TFContextRDDMethods[K, D]

  implicit class WithTFSTContextRDDMethods[D: TypeTag](
    val self: RDD[(SpaceTimeKey, TileFeature[Tile, D])] with Metadata[TileLayerMetadata[SpaceTimeKey]]
  )(implicit spark: SparkSession)
      extends TFSTContextRDDMethods[D]

  private[astraea] implicit class WithMetadataMethods[R: JsonFormat](val self: R)
      extends MetadataMethods[R]

  type TileFeatureLayerRDD[K, D] =
    RDD[(K, TileFeature[Tile, D])] with Metadata[TileLayerMetadata[K]]

  object TileFeatureLayerRDD {
    def apply[K, D](rdd: RDD[(K, TileFeature[Tile, D])],
                    metadata: TileLayerMetadata[K]): TileFeatureLayerRDD[K, D] =
      new ContextRDD(rdd, metadata)
  }

  private[rasterframes] implicit class WithWiden[A, B](thing: Either[A, B]) {
    /** Returns the value as a LUB of the Left & Right items. */
    def widen[Out](implicit ev: Lub[A, B, Out]): Out =
      thing.fold(identity, identity).asInstanceOf[Out]
  }

  private[rasterframes] implicit class WithCombine[T](left: Option[T]) {
    def combine[A, R >: A](a: A)(f: (T, A) ⇒ R): R = left.map(f(_, a)).getOrElse(a)
    def tupleWith[R](right: Option[R]): Option[(T, R)] = left.flatMap(l ⇒ right.map((l, _)))
  }

  implicit class NamedColumn(col: Column) {
    def columnName: String = col.expr match {
      case ua: UnresolvedAttribute ⇒ ua.name
      case ar: AttributeReference ⇒ ar.name
      case as: Alias ⇒ as.name
      case o ⇒ o.prettyName
    }
  }
}
