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

package org.locationtech.rasterframes

import com.typesafe.scalalogging.Logger
import geotrellis.layer._
import geotrellis.raster.crop.TileCropMethods
import geotrellis.raster.mapalgebra.local.LocalTileBinaryOp
import geotrellis.raster.mask.TileMaskMethods
import geotrellis.raster.merge.TileMergeMethods
import geotrellis.raster.prototype.TilePrototypeMethods
import geotrellis.raster.render.{ColorRamp, ColorRamps}
import geotrellis.raster.{CellGrid, Grid, GridBounds, TargetCell}
import geotrellis.spark.tiling.TilerKeyMethods
import geotrellis.util.GetComponent
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.rf._
import org.apache.spark.sql.types.StringType
import org.slf4j.LoggerFactory
import spire.math.Integral

/**
 * Internal utilities.
 *
 * @since 12/18/17
 */
package object util extends DataFrameRenderers {
  // Don't make this a `lazy val`... breaks Spark assemblies for some reason.
  protected def logger: Logger = Logger(LoggerFactory.getLogger("org.locationtech.rasterframes"))

  import reflect.ClassTag
  import reflect.runtime.universe._

  implicit class TypeTagCanBeClassTag[T](val t: TypeTag[T]) extends AnyVal {
    def asClassTag: ClassTag[T] = ClassTag[T](t.mirror.runtimeClass(t.tpe))
  }

  implicit class GridHasGridBounds[N: Integral](re: Grid[N]) {
    import spire.syntax.integral._
    val in = Integral[N]
    def gridBounds: GridBounds[N] = GridBounds(in.zero, in.zero, re.cols - in.one, re.rows - in.one)
  }

  /**
   * Type lambda alias for components that have bounds with parameterized key.
   * @tparam K bounds key type
   */
  type BoundsComponentOf[K] = {
    type Get[M] = GetComponent[M, Bounds[K]]
  }

  // Type lambda aliases
  type WithMergeMethods[V] = V => TileMergeMethods[V]
  type WithPrototypeMethods[V <: CellGrid[Int]] = V => TilePrototypeMethods[V]
  type WithCropMethods[V <: CellGrid[Int]] = V => TileCropMethods[V]
  type WithMaskMethods[V] = V => TileMaskMethods[V]

  type KeyMethodsProvider[K1, K2] = K1 => TilerKeyMethods[K1, K2]

  /** Internal method for slapping the RasterFrameLayer seal of approval on a DataFrame. */
  private[rasterframes] def certifyLayer(df: DataFrame): RasterFrameLayer =
    shapeless.tag[RasterFrameTag][DataFrame](df)


  /** Tags output column with a nicer name. */
  private[rasterframes]
  def withAlias(name: String, inputs: Column*)(output: Column): Column = {
    val paramNames = inputs.map(_.columnName).mkString(",")
    output.as(s"$name($paramNames)")
  }

  /** Tags output column with a nicer name, yet strongly typed. */
  private[rasterframes]
  def withTypedAlias[T: Encoder](name: String, inputs: Column*)(output: Column): TypedColumn[Any, T] =
    withAlias(name, inputs: _*)(output).as[T]

  /** Derives and operator name from the implementing object name. */
  private[rasterframes]
  def opName(op: LocalTileBinaryOp) =
    op.getClass.getSimpleName.replace("$", "").toLowerCase

  implicit class WithCombine[T](left: Option[T]) {
    def combine[A, R >: A](a: A)(f: (T, A) => R): R = left.map(f(_, a)).getOrElse(a)
    def tupleWith[R](right: Option[R]): Option[(T, R)] = left.flatMap(l => right.map((l, _)))
  }

  implicit class ExpressionWithName(val expr: Expression) extends AnyVal {
    import org.apache.spark.sql.catalyst.expressions.Literal
    def name: String = expr match {
      case n: NamedExpression if n.resolved => n.name
      case UnresolvedAttribute(parts) => parts.mkString("_")
      case Alias(_, name) => name
      case l: Literal if l.dataType == StringType => String.valueOf(l.value)
      case o => o.toString
    }
  }

  implicit class NamedColumn(val col: Column) extends AnyVal {
    def columnName: String = col.expr.name
  }

  private[rasterframes]
  implicit class Pipeable[A](val a: A) extends AnyVal {
    def |>[B](f: A => B): B = f(a)
  }

  /** Applies the given thunk to the closable resource. */
  def withResource[T <: CloseLike, R](t: T)(thunk: T => R): R = {
    import scala.language.reflectiveCalls
    try { thunk(t) } finally { t.close() }
  }

  /** Report the time via slf4j it takes to execute a code block. Annotated with given tag. */
  def time[R](tag: String)(block: => R): R = {
    val start = System.currentTimeMillis()
    val result = block
    val end = System.currentTimeMillis()
    logger.info(f"Elapsed time of $tag - ${(end - start)*1e-3}%.4fs")
    result
  }

  /** Anything that structurally has a close method. */
  type CloseLike = { def close(): Unit }

  implicit class Conditionalize[T](left: T) {
    def when(pred: T => Boolean): Option[T] = Option(left).filter(pred)
  }

  implicit class ConditionalApply[T](val left: T) extends AnyVal {
    def applyWhen[R >: T](pred: T => Boolean, f: T => R): R = if(pred(left)) f(left) else left
  }

  object ColorRampNames {
    import ColorRamps._
    private lazy val mapping = Map(
      "BlueToOrange" -> BlueToOrange,
      "LightYellowToOrange" -> LightYellowToOrange,
      "BlueToRed" -> BlueToRed,
      "GreenToRedOrange" -> GreenToRedOrange,
      "LightToDarkSunset" -> LightToDarkSunset,
      "LightToDarkGreen" -> LightToDarkGreen,
      "HeatmapYellowToRed" -> HeatmapYellowToRed,
      "HeatmapBlueToYellowToRedSpectrum" -> HeatmapBlueToYellowToRedSpectrum,
      "HeatmapDarkRedToYellowWhite" -> HeatmapDarkRedToYellowWhite,
      "HeatmapLightPurpleToDarkPurpleToWhite" -> HeatmapLightPurpleToDarkPurpleToWhite,
      "ClassificationBoldLandUse" -> ClassificationBoldLandUse,
      "ClassificationMutedTerrain" -> ClassificationMutedTerrain,
      "Magma" -> Magma,
      "Inferno" -> Inferno,
      "Plasma" -> Plasma,
      "Viridis" -> Viridis,
      "Greyscale2"-> greyscale(2),
      "Greyscale8"-> greyscale(8),
      "Greyscale32"-> greyscale(32),
      "Greyscale64"-> greyscale(64),
      "Greyscale128"-> greyscale(128),
      "Greyscale256"-> greyscale(256)
    )

    def unapply(name: String): Option[ColorRamp] = mapping.get(name)

    def apply() = mapping.keys.toSeq
  }

  object FocalNeighborhood {
    import scala.util.Try
    import geotrellis.raster.Neighborhood
    import geotrellis.raster.mapalgebra.focal._

    // pattern matching and string interpolation works only since Scala 2.13
    def fromString(name: String): Try[Neighborhood] = Try {
      name.toLowerCase().trim() match {
        case s if s.startsWith("square-") => Square(Integer.parseInt(s.split("square-").last))
        case s if s.startsWith("circle-") => Circle(java.lang.Double.parseDouble(s.split("circle-").last))
        case s if s.startsWith("nesw-") => Nesw(Integer.parseInt(s.split("nesw-").last))
        case s if s.startsWith("wedge-") => {
          val List(radius: Double, startAngle: Double, endAngle: Double) =
            s
              .split("wedge-")
              .last
              .split("-")
              .toList
              .map(java.lang.Double.parseDouble)

          Wedge(radius, startAngle, endAngle)
        }

        case s if s.startsWith("annulus-") => {
          val List(innerRadius: Double, outerRadius: Double) =
            s
              .split("annulus-")
              .last
              .split("-")
              .toList
              .map(java.lang.Double.parseDouble)

          Annulus(innerRadius, outerRadius)
        }
        case _ => throw new IllegalArgumentException(s"Unrecognized Neighborhood $name")
      }
    }

    def apply(neighborhood: Neighborhood): String = {
      neighborhood match {
        case Square(e)                           => s"square-$e"
        case Circle(e)                           => s"circle-$e"
        case Nesw(e)                             => s"nesw-$e"
        case Wedge(radius, startAngle, endAngle) => s"nesw-$radius-$startAngle-$endAngle"
        case Annulus(innerRadius, outerRadius)   => s"annulus-$innerRadius-$outerRadius"
        case _                                   => throw new IllegalArgumentException(s"Unrecognized Neighborhood ${neighborhood.toString}")
      }
    }
  }

  object ResampleMethod {
    import geotrellis.raster.resample.{ResampleMethod => GTResampleMethod, _}
    def unapply(name: String): Option[GTResampleMethod] = {
      name.toLowerCase().trim().replaceAll("_", "") match {
        case "nearestneighbor" | "nearest" => Some(NearestNeighbor)
        case "bilinear" => Some(Bilinear)
        case "cubicconvolution" => Some(CubicConvolution)
        case "cubicspline" => Some(CubicSpline)
        case "lanczos" | "lanzos" => Some(Lanczos)
        // aggregates
        case "average" => Some(Average)
        case "mode" => Some(Mode)
        case "median" => Some(Median)
        case "max" => Some(Max)
        case "min" => Some(Min)
        case "sum" => Some(Sum)
        case _ => None
      }
    }
    def apply(gtr: GTResampleMethod): String = {
      gtr match {
        case NearestNeighbor => "nearest"
        case Bilinear => "bilinear"
        case CubicConvolution => "cubicconvolution"
        case CubicSpline => "cubicspline"
        case Lanczos => "lanczos"
        case Average => "average"
        case Mode => "mode"
        case Median => "median"
        case Max => "max"
        case Min => "min"
        case Sum => "sum"
        case _ => throw new IllegalArgumentException(s"Unrecognized ResampleMethod ${gtr.toString}")
      }
    }
  }

  object FocalTargetCell {
    def fromString(str: String): TargetCell = str.toLowerCase match {
      case "nodata" => TargetCell.NoData
      case "data" => TargetCell.Data
      case "all" => TargetCell.All
      case _ => throw new IllegalArgumentException(s"Unrecognized TargetCell $str")
    }

    def apply(tc: TargetCell): String = tc match {
      case TargetCell.NoData => "nodata"
      case TargetCell.Data => "data"
      case TargetCell.All => "all"
    }
  }

  private[rasterframes]
  def toParquetFriendlyColumnName(name: String) = name.replaceAll("[ ,;{}()\n\t=]", "_")

  def registerResolution(sqlContext: SQLContext, rule: Rule[LogicalPlan]): Unit = {
    logger.error("Extended rule resolution not available in this version of Spark")
    analyzer(sqlContext).extendedResolutionRules
  }
}
