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

import geotrellis.raster.{DoubleConstantNoDataCellType, Tile}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistryBase
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ExpressionInfo, ScalaUDF}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow, ScalaReflection}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.SQLContext
import org.locationtech.rasterframes.expressions.accessors._
import org.locationtech.rasterframes.expressions.aggregates.CellCountAggregate.DataCells
import org.locationtech.rasterframes.expressions.aggregates._
import org.locationtech.rasterframes.expressions.generators._
import org.locationtech.rasterframes.expressions.localops._
import org.locationtech.rasterframes.expressions.focalops._
import org.locationtech.rasterframes.expressions.tilestats._
import org.locationtech.rasterframes.expressions.transformers._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Module of Catalyst expressions for efficiently working with tiles.
 *
 * @since 10/10/17
 */
package object expressions {
  type HasTernaryExpressionCopy = {def copy(first: Expression, second: Expression, third: Expression): Expression}
  type HasBinaryExpressionCopy = {def copy(left: Expression, right: Expression): Expression}
  type HasUnaryExpressionCopy = {def copy(child: Expression): Expression}

  private[expressions] def row(input: Any) = input.asInstanceOf[InternalRow]
  /** Convert the tile to a floating point type as needed for scalar operations. */
  @inline
  private[expressions]
  def fpTile(t: Tile) = if (t.cellType.isFloatingPoint) t else t.convert(DoubleConstantNoDataCellType)

  /**
   * As opposed to `udf`, this constructs an unwrapped ScalaUDF Expression from a function.
   * This ScalaUDF Expression expects the argument of type A1 to match the return type RT at runtime.
   */
  private[expressions]
  def udfiexpr[RT: TypeTag, A1: TypeTag](name: String, f: DataType => A1 => RT): Expression => ScalaUDF = (child: Expression) => {
    val ScalaReflection.Schema(dataType, _) = ScalaReflection.schemaFor[RT]
    ScalaUDF((row: A1) => f(child.dataType)(row), dataType, Seq(child), Seq(Option(ExpressionEncoder[RT]().resolveAndBind())), udfName = Some(name))

  }

  private def expressionInfo[T : ClassTag](name: String, since: Option[String], database: Option[String]): ExpressionInfo = {
    val clazz = scala.reflect.classTag[T].runtimeClass
    val df = clazz.getAnnotation(classOf[ExpressionDescription])
    if (df != null) {
      if (df.extended().isEmpty) {
        new ExpressionInfo(
          clazz.getCanonicalName,
          database.orNull,
          name,
          df.usage(),
          df.arguments(),
          df.examples(),
          df.note(),
          df.group(),
          since.getOrElse(df.since()),
          df.deprecated(),
          df.source())
      } else {
        // This exists for the backward compatibility with old `ExpressionDescription`s defining
        // the extended description in `extended()`.
        new ExpressionInfo(clazz.getCanonicalName, database.orNull, name, df.usage(), df.extended())
      }
    } else {
      new ExpressionInfo(clazz.getCanonicalName, name)
    }
  }

  def register(sqlContext: SQLContext, database: Option[String] = None): Unit = {
    val registry = sqlContext.sparkSession.sessionState.functionRegistry

    def registerFunction[T <: Expression : ClassTag](name: String, since: Option[String] = None)(builder: Seq[Expression] => T): Unit = {
      val id = FunctionIdentifier(name, database)
      val info = FunctionRegistryBase.expressionInfo[T](name, since)
      registry.registerFunction(id, info, builder)
    }

    def register1[T <: Expression : ClassTag](
      name: String,
      builder: Expression => T
    ): Unit = registerFunction[T](name, None){ args => builder(args(0))
    }

    def register2[T <: Expression : ClassTag](
      name: String,
      builder: (Expression, Expression) => T
    ): Unit = registerFunction[T](name, None){ args => builder(args(0), args(1)) }

    def register3[T <: Expression : ClassTag](
      name: String,
      builder: (Expression, Expression, Expression) => T
    ): Unit = registerFunction[T](name, None){ args => builder(args(0), args(1), args(2)) }

    def register5[T <: Expression : ClassTag](
      name: String,
      builder: (Expression, Expression, Expression, Expression, Expression) => T
    ): Unit = registerFunction[T](name, None){ args => builder(args(0), args(1), args(2), args(3), args(4)) }

    register2("rf_local_add", Add(_, _))
    register2("rf_local_subtract", Subtract(_, _))
    registerFunction("rf_explode_tiles"){ExplodeTiles(1.0, None, _)}
    register5("rf_assemble_tile", TileAssembler(_, _, _, _, _))
    register1("rf_cell_type", GetCellType(_))
    register2("rf_convert_cell_type", SetCellType(_, _))
    register2("rf_interpret_cell_type_as", InterpretAs(_, _))
    register2("rf_with_no_data", SetNoDataValue(_,_))
    register1("rf_dimensions", GetDimensions(_))
    register1("st_geometry", ExtentToGeometry(_))
    register1("rf_geometry", GetGeometry(_))
    register1("st_extent", GeometryToExtent(_))
    register1("rf_extent", GetExtent(_))
    register1("rf_crs", GetCRS(_))
    register1("rf_tile", RealizeTile(_))
    register3("rf_proj_raster", CreateProjectedRaster(_, _, _))
    register2("rf_local_multiply", Multiply(_, _))
    register2("rf_local_divide", Divide(_, _))
    register2("rf_normalized_difference", NormalizedDifference(_,_))
    register2("rf_local_less", Less(_, _))
    register2("rf_local_greater", Greater(_, _))
    register2("rf_local_less_equal", LessEqual(_, _))
    register2("rf_local_greater_equal", GreaterEqual(_, _))
    register2("rf_local_equal", Equal(_, _))
    register2("rf_local_unequal", Unequal(_, _))
    register2("rf_local_is_in", IsIn(_, _))
    register1("rf_local_no_data", Undefined(_))
    register1("rf_local_data", Defined(_))
    register2("rf_local_min", Min(_, _))
    register2("rf_local_max", Max(_, _))
    register3("rf_local_clamp", Clamp(_, _, _))
    register3("rf_where", Where(_, _, _))
    register3("rf_standardize", Standardize(_, _, _))
    register3("rf_rescale", Rescale(_, _ , _))
    register1("rf_tile_sum", Sum(_))
    register1("rf_round", Round(_))
    register1("rf_abs", Abs(_))
    register1("rf_log", Log(_))
    register1("rf_log10", Log10(_))
    register1("rf_log2", Log2(_))
    register1("rf_log1p", Log1p(_))
    register1("rf_exp", Exp(_))
    register1("rf_exp10", Exp10(_))
    register1("rf_exp2", Exp2(_))
    register1("rf_expm1", ExpM1(_))
    register1("rf_sqrt", Sqrt(_))
    register3("rf_resample", Resample(_, _, _))
    register2("rf_resample_nearest", ResampleNearest(_, _))
    register1("rf_tile_to_array_double", TileToArrayDouble(_))
    register1("rf_tile_to_array_int", TileToArrayInt(_))
    register1("rf_data_cells", DataCells(_))
    register1("rf_no_data_cells", NoDataCells(_))
    register1("rf_is_no_data_tile", IsNoDataTile(_))
    register1("rf_exists", Exists(_))
    register1("rf_for_all", ForAll(_))
    register1("rf_tile_min", TileMin(_))
    register1("rf_tile_max", TileMax(_))
    register1("rf_tile_mean", TileMean(_))
    register1("rf_tile_stats", TileStats(_))
    register1("rf_tile_histogram", TileHistogram(_))
    register1("rf_agg_data_cells", DataCells(_))
    register1("rf_agg_no_data_cells", CellCountAggregate.NoDataCells(_))
    register1("rf_agg_stats", CellStatsAggregate.CellStatsAggregateUDAF(_))
    register1("rf_agg_approx_histogram", HistogramAggregate.HistogramAggregateUDAF(_))
    register1("rf_agg_local_stats", LocalStatsAggregate.LocalStatsAggregateUDAF(_))
    register1("rf_agg_local_min",LocalTileOpAggregate.LocalMinUDAF(_))
    register1("rf_agg_local_max", LocalTileOpAggregate.LocalMaxUDAF(_))
    register1("rf_agg_local_data_cells", LocalCountAggregate.LocalDataCellsUDAF(_))
    register1("rf_agg_local_no_data_cells", LocalCountAggregate.LocalNoDataCellsUDAF(_))
    register1("rf_agg_local_mean", LocalMeanAggregate(_))
    register3(FocalMax.name, FocalMax(_, _, _))
    register3(FocalMin.name, FocalMin(_, _, _))
    register3(FocalMean.name, FocalMean(_, _, _))
    register3(FocalMode.name, FocalMode(_, _, _))
    register3(FocalMedian.name, FocalMedian(_, _, _))
    register3(FocalMoransI.name, FocalMoransI(_, _, _))
    register3(FocalStdDev.name, FocalStdDev(_, _, _))
    register3(Convolve.name, Convolve(_, _, _))

    register3(Slope.name, Slope(_, _, _))
    register2(Aspect.name, Aspect(_, _))
    register5(Hillshade.name, Hillshade(_, _, _, _, _))

    register2("rf_mask", MaskByDefined(_, _))
    register2("rf_inverse_mask", InverseMaskByDefined(_, _))
    register3("rf_mask_by_value", MaskByValue(_, _, _))
    register3("rf_inverse_mask_by_value", InverseMaskByValue(_, _, _))
    register3("rf_mask_by_values", MaskByValues(_, _, _))

    register1("rf_render_ascii", DebugRender.RenderAscii(_))
    register1("rf_render_matrix", DebugRender.RenderMatrix(_))
    register1("rf_render_png", RenderPNG.RenderCompositePNG(_))
    register3("rf_rgb_composite", RGBComposite(_, _, _))

    register2("rf_xz2_index", XZ2Indexer(_, _, 18.toShort))
    register2("rf_z2_index", Z2Indexer(_, _, 31.toShort))

    register3("st_reproject", ReprojectGeometry(_, _, _))

    register3[ExtractBits]("rf_local_extract_bits", ExtractBits(_: Expression, _: Expression, _: Expression))
    register3[ExtractBits]("rf_local_extract_bit", ExtractBits(_: Expression, _: Expression, _: Expression))
  }
}
