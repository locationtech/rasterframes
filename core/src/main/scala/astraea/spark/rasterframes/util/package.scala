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

package astraea.spark.rasterframes

import geotrellis.proj4.WebMercator
import geotrellis.raster.crop.TileCropMethods
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import geotrellis.raster.{CellGrid, MultibandTile, Tile, TileLayout}
import geotrellis.raster.mapalgebra.local.LocalTileBinaryOp
import geotrellis.raster.mask.TileMaskMethods
import geotrellis.raster.merge.TileMergeMethods
import geotrellis.raster.prototype.TilePrototypeMethods
import geotrellis.spark.io.slippy.HadoopSlippyTileWriter
import geotrellis.spark.tiling.{TilerKeyMethods, ZoomedLayoutScheme}
import geotrellis.spark.{Bounds, ContextRDD, KeyBounds, MultibandTileLayerRDD, SpaceTimeKey, SpatialComponent, SpatialKey, TileLayerMetadata, TileLayerRDD}
import geotrellis.util.{GetComponent, LazyLogging}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.rf._
import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import shapeless.Lub

/**
 * Internal utilities.
 *
 * @since 12/18/17
 */
package object util extends LazyLogging {

  /**
   * Type lambda alias for components that have bounds with parameterized key.
   * @tparam K bounds key type
   */
  type BoundsComponentOf[K] = {
    type Get[M] = GetComponent[M, Bounds[K]]
  }

  // Type lambda aliases
  type WithMergeMethods[V] = (V ⇒ TileMergeMethods[V])
  type WithPrototypeMethods[V <: CellGrid] = (V ⇒ TilePrototypeMethods[V])
  type WithCropMethods[V <: CellGrid] = (V ⇒ TileCropMethods[V])
  type WithMaskMethods[V] = (V ⇒ TileMaskMethods[V])

  type KeyMethodsProvider[K1, K2] = K1 ⇒ TilerKeyMethods[K1, K2]

  /** Internal method for slapping the RasterFrame seal of approval on a DataFrame. */
  private[rasterframes] def certifyRasterframe(df: DataFrame): RasterFrame =
    shapeless.tag[RasterFrameTag][DataFrame](df)


  /** Tags output column with a nicer name. */
  private[rasterframes]
  def withAlias(name: String, inputs: Column*)(output: Column) = {
    val paramNames = inputs.map(_.columnName).mkString(",")
    output.as(s"$name($paramNames)")
  }

  /** Derives and operator name from the implementing object name. */
  private[rasterframes]
  def opName(op: LocalTileBinaryOp) =
    op.getClass.getSimpleName.replace("$", "").toLowerCase


  // $COVERAGE-OFF$
  implicit class WithWiden[A, B](thing: Either[A, B]) {

    /** Returns the value as a LUB of the Left & Right items. */
    def widen[Out](implicit ev: Lub[A, B, Out]): Out =
      thing.fold(identity, identity).asInstanceOf[Out]
  }

  implicit class WithCombine[T](left: Option[T]) {
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

  private[rasterframes]
  implicit class Pipeable[A](val a: A) extends AnyVal {
    def |>[B](f: A ⇒ B): B = f(a)
  }

  /** Applies the given thunk to the closable resource. */
  def withResource[T <: CloseLike, R](t: T)(thunk: T ⇒ R): R = {
    import scala.language.reflectiveCalls
    try { thunk(t) } finally { t.close() }
  }

  /** Anything that structurally has a close method. */
  type CloseLike = { def close(): Unit }

  implicit class Conditionalize[T](left: T) {
    def when(pred: T ⇒ Boolean): Option[T] = Option(left).filter(pred)
  }

  implicit class ConditionalMap[T](val left: T) extends AnyVal {
    def mapWhen[R >: T](pred: T ⇒ Boolean, f: T ⇒ R): R = if(pred(left)) f(left) else left
  }

  private[rasterframes]
  def toParquetFriendlyColumnName(name: String) = name.replaceAll("[ ,;{}()\n\t=]", "_")

  def registerResolution(sqlContext: SQLContext, rule: Rule[LogicalPlan]): Unit = {
    logger.error("Extended rule resolution not available in this version of Spark")
    analyzer(sqlContext).extendedResolutionRules
  }
  // $COVERAGE-ON$
}
