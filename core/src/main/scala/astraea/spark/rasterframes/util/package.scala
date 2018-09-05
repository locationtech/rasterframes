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

import geotrellis.proj4.CRS
import geotrellis.raster.CellGrid
import geotrellis.raster.crop.TileCropMethods
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.mapalgebra.local.LocalTileBinaryOp
import geotrellis.raster.mask.TileMaskMethods
import geotrellis.raster.merge.TileMergeMethods
import geotrellis.raster.prototype.TilePrototypeMethods
import geotrellis.spark.Bounds
import geotrellis.spark.tiling.TilerKeyMethods
import geotrellis.util.{ByteReader, GetComponent, LazyLogging}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.rf._
import org.apache.spark.sql.{Column, DataFrame, SQLContext}

import scala.util.control.NonFatal

/**
 * Internal utilities.
 *
 * @since 12/18/17
 */
package object util extends LazyLogging {

  import reflect.ClassTag
  import reflect.runtime.universe._

  implicit class TypeTagCanBeClassTag[T](val t: TypeTag[T]) extends AnyVal {
    def asClassTag: ClassTag[T] = ClassTag[T](t.mirror.runtimeClass(t.tpe))
  }

  /**
   * Type lambda alias for components that have bounds with parameterized key.
   * @tparam K bounds key type
   */
  type BoundsComponentOf[K] = {
    type Get[M] = GetComponent[M, Bounds[K]]
  }

  // Type lambda aliases
  type WithMergeMethods[V] = V ⇒ TileMergeMethods[V]
  type WithPrototypeMethods[V <: CellGrid] = V ⇒ TilePrototypeMethods[V]
  type WithCropMethods[V <: CellGrid] = V ⇒ TileCropMethods[V]
  type WithMaskMethods[V] = V ⇒ TileMaskMethods[V]

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

  object CRSParser {
    def apply(value: String): CRS = scala.util.Try(CRS.fromName(value))
        .recover { case NonFatal(_) ⇒ CRS.fromString(value)}
        .getOrElse(CRS.fromWKT(value))
  }

  implicit class WithCombine[T](left: Option[T]) {
    def combine[A, R >: A](a: A)(f: (T, A) ⇒ R): R = left.map(f(_, a)).getOrElse(a)
    def tupleWith[R](right: Option[R]): Option[(T, R)] = left.flatMap(l ⇒ right.map((l, _)))
  }

  implicit class NamedExpression(val expr: Expression) extends AnyVal {
    def name: String = expr match {
      case ua: UnresolvedAttribute ⇒ ua.name
      case ar: AttributeReference ⇒ ar.name
      case as: Alias ⇒ as.name
      case o ⇒ o.prettyName
    }
  }

  implicit class NamedColumn(val col: Column) extends AnyVal {
    def columnName: String = col.expr.name
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

  /** Report the time via slf4j it takes to execute a code block. Annotated with given tag. */
  def time[R](tag: String)(block: ⇒ R): R = {
    val start = System.currentTimeMillis()
    val result = block
    val end = System.currentTimeMillis()
    logger.info(f"Elapsed time of $tag - ${(end - start)*1e-3}%.4fs")
    result
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

  object Shims {
    // GT 1.2.1 to 2.0.0
    def toArrayTile[T <: CellGrid](tile: T): T =
      tile.getClass.getMethods
        .find(_.getName == "toArrayTile")
        .map(_.invoke(tile).asInstanceOf[T])
        .getOrElse(tile)

    // GT 1.2.1 to 2.0.0
    def merge[V <: CellGrid: ClassTag: WithMergeMethods](left: V, right: V, col: Int, row: Int): V = {
      val merger = implicitly[WithMergeMethods[V]].apply(left)
      merger.getClass.getDeclaredMethods
        .find(m ⇒ m.getName == "merge" && m.getParameterCount == 3)
        .map(_.invoke(merger, right, Int.box(col), Int.box(row)).asInstanceOf[V])
        .getOrElse(merger.merge(right))
    }

    // GT 1.2.1 to 2.0.0
    def readGeoTiffInfo(byteReader: ByteReader, decompress: Boolean, streaming: Boolean): GeoTiffReader.GeoTiffInfo = {
      val reader = GeoTiffReader.getClass.getDeclaredMethods
        .find(_.getName == "readGeoTiffInfo")
        .getOrElse(throw new RuntimeException("Could not find method GeoTiffReader.readGeoTiffInfo"))

      val result = reader.getParameterCount match {
        case 3 ⇒ reader.invoke(GeoTiffReader, byteReader,
          Boolean.box(decompress), Boolean.box(streaming))
        case 4 ⇒ reader.invoke(GeoTiffReader, byteReader,
          Boolean.box(decompress), Boolean.box(streaming), None)
      }
      result.asInstanceOf[GeoTiffReader.GeoTiffInfo]
    }
  }

}
