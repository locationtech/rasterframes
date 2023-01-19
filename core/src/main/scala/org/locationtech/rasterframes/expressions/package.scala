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
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
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
import shapeless.HList
import shapeless.ops.function.FnToProduct
import shapeless.ops.traversable.FromTraversable
import shapeless.syntax.std.function._
import shapeless.syntax.std.traversable._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.language.implicitConversions

/**
 * Module of Catalyst expressions for efficiently working with tiles.
 *
 * @since 10/10/17
 */
package object expressions {
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

  def register(sqlContext: SQLContext, database: Option[String] = None): Unit = {
    val registry = sqlContext.sparkSession.sessionState.functionRegistry

    def registerFunction[T <: Expression : ClassTag](name: String, since: Option[String] = None)(builder: Seq[Expression] => T): Unit = {
      val id = FunctionIdentifier(name, database)
      val info = FunctionRegistryBase.expressionInfo[T](name, since)
      registry.registerFunction(id, info, builder)
    }

    /** Converts (expr1: Expression, ..., exprn: Expression) => R into a Seq[Expression] => R function */
    implicit def expressionArgumentsSequencer[F, L <: HList, R](f: F)(implicit ftp: FnToProduct.Aux[F, L => R], ft: FromTraversable[L]): Seq[Expression] => R = { list: Seq[Expression] =>
      list.toHList match {
        case Some(l) => f.toProduct(l)
        case None => throw new IllegalArgumentException(s"registerFunction application failed; arity mismatch: $list.")
      }
    }

    registerFunction[Add](name = "rf_local_add")(Add.apply)
    registerFunction[Subtract](name = "rf_local_subtract")(Subtract.apply)
    registerFunction[ExplodeTiles](name = "rf_explode_tiles")(ExplodeTiles(1.0, None, _))
    registerFunction[TileAssembler](name = "rf_assemble_tile")(TileAssembler(_: Expression, _: Expression, _: Expression, _: Expression, _: Expression))
    registerFunction[GetCellType](name = "rf_cell_type")(GetCellType.apply)
    registerFunction[SetCellType](name = "rf_convert_cell_type")(SetCellType.apply)
    registerFunction[InterpretAs](name = "rf_interpret_cell_type_as")(InterpretAs.apply)
    registerFunction[SetNoDataValue](name = "rf_with_no_data")(SetNoDataValue.apply)
    registerFunction[GetDimensions](name = "rf_dimensions")(GetDimensions.apply)
    registerFunction[ExtentToGeometry](name = "st_geometry")(ExtentToGeometry.apply)
    registerFunction[GetGeometry](name = "rf_geometry")(GetGeometry.apply)
    registerFunction[GeometryToExtent](name = "st_extent")(GeometryToExtent.apply)
    registerFunction[GetExtent](name = "rf_extent")(GetExtent.apply)
    registerFunction[GetCRS](name = "rf_crs")(GetCRS.apply)
    registerFunction[RealizeTile](name = "rf_tile")(RealizeTile.apply)
    registerFunction[CreateProjectedRaster](name = "rf_proj_raster")(CreateProjectedRaster.apply)
    registerFunction[Multiply](name = "rf_local_multiply")(Multiply.apply)
    registerFunction[Divide](name = "rf_local_divide")(Divide.apply)
    registerFunction[NormalizedDifference](name = "rf_normalized_difference")(NormalizedDifference.apply)
    registerFunction[Less](name = "rf_local_less")(Less.apply)
    registerFunction[Greater](name = "rf_local_greater")(Greater.apply)
    registerFunction[LessEqual](name = "rf_local_less_equal")(LessEqual.apply)
    registerFunction[GreaterEqual](name = "rf_local_greater_equal")(GreaterEqual.apply)
    registerFunction[Equal](name = "rf_local_equal")(Equal.apply)
    registerFunction[Unequal](name = "rf_local_unequal")(Unequal.apply)
    registerFunction[IsIn](name = "rf_local_is_in")(IsIn.apply)
    registerFunction[Undefined](name = "rf_local_no_data")(Undefined.apply)
    registerFunction[Defined](name = "rf_local_data")(Defined.apply)
    registerFunction[Min](name = "rf_local_min")(Min.apply)
    registerFunction[Max](name = "rf_local_max")(Max.apply)
    registerFunction[Clamp](name = "rf_local_clamp")(Clamp.apply)
    registerFunction[Where](name = "rf_where")(Where.apply)
    registerFunction[Standardize](name = "rf_standardize")(Standardize.apply)
    registerFunction[Rescale](name = "rf_rescale")(Rescale.apply)
    registerFunction[Sum](name = "rf_tile_sum")(Sum.apply)
    registerFunction[Round](name = "rf_round")(Round.apply)
    registerFunction[Abs](name = "rf_abs")(Abs.apply)
    registerFunction[Log](name = "rf_log")(Log.apply)
    registerFunction[Log10](name = "rf_log10")(Log10.apply)
    registerFunction[Log2](name = "rf_log2")(Log2.apply)
    registerFunction[Log1p](name = "rf_log1p")(Log1p.apply)
    registerFunction[Exp](name = "rf_exp")(Exp.apply)
    registerFunction[Exp10](name = "rf_exp10")(Exp10.apply)
    registerFunction[Exp2](name = "rf_exp2")(Exp2.apply)
    registerFunction[ExpM1](name = "rf_expm1")(ExpM1.apply)
    registerFunction[Sqrt](name = "rf_sqrt")(Sqrt.apply)
    registerFunction[Resample](name = "rf_resample")(Resample.apply)
    registerFunction[ResampleNearest](name = "rf_resample_nearest")(ResampleNearest.apply)
    registerFunction[TileToArrayDouble](name = "rf_tile_to_array_double")(TileToArrayDouble.apply)
    registerFunction[TileToArrayInt](name = "rf_tile_to_array_int")(TileToArrayInt.apply)
    registerFunction[DataCells](name = "rf_data_cells")(DataCells.apply)
    registerFunction[NoDataCells](name = "rf_no_data_cells")(NoDataCells.apply)
    registerFunction[IsNoDataTile](name = "rf_is_no_data_tile")(IsNoDataTile.apply)
    registerFunction[Exists](name = "rf_exists")(Exists.apply)
    registerFunction[ForAll](name = "rf_for_all")(ForAll.apply)
    registerFunction[TileMin](name = "rf_tile_min")(TileMin.apply)
    registerFunction[TileMax](name = "rf_tile_max")(TileMax.apply)
    registerFunction[TileMean](name = "rf_tile_mean")(TileMean.apply)
    registerFunction[TileStats](name = "rf_tile_stats")(TileStats.apply)
    registerFunction[TileHistogram](name = "rf_tile_histogram")(TileHistogram.apply)
    registerFunction[DataCells](name = "rf_agg_data_cells")(DataCells.apply)
    registerFunction[CellCountAggregate.NoDataCells](name = "rf_agg_no_data_cells")(CellCountAggregate.NoDataCells.apply)
    registerFunction[CellStatsAggregate.CellStatsAggregateUDAF](name = "rf_agg_stats")(CellStatsAggregate.CellStatsAggregateUDAF.apply)
    registerFunction[HistogramAggregate.HistogramAggregateUDAF](name = "rf_agg_approx_histogram")(HistogramAggregate.HistogramAggregateUDAF.apply)
    registerFunction[LocalStatsAggregate.LocalStatsAggregateUDAF](name = "rf_agg_local_stats")(LocalStatsAggregate.LocalStatsAggregateUDAF.apply)
    registerFunction[LocalTileOpAggregate.LocalMinUDAF](name = "rf_agg_local_min")(LocalTileOpAggregate.LocalMinUDAF.apply)
    registerFunction[LocalTileOpAggregate.LocalMaxUDAF](name = "rf_agg_local_max")(LocalTileOpAggregate.LocalMaxUDAF.apply)
    registerFunction[LocalCountAggregate.LocalDataCellsUDAF](name = "rf_agg_local_data_cells")(LocalCountAggregate.LocalDataCellsUDAF.apply)
    registerFunction[LocalCountAggregate.LocalNoDataCellsUDAF](name = "rf_agg_local_no_data_cells")(LocalCountAggregate.LocalNoDataCellsUDAF.apply)
    registerFunction[LocalMeanAggregate](name = "rf_agg_local_mean")(LocalMeanAggregate.apply)
    registerFunction[FocalMax](FocalMax.name)(FocalMax.apply)
    registerFunction[FocalMin](FocalMin.name)(FocalMin.apply)
    registerFunction[FocalMean](FocalMean.name)(FocalMean.apply)
    registerFunction[FocalMode](FocalMode.name)(FocalMode.apply)
    registerFunction[FocalMedian](FocalMedian.name)(FocalMedian.apply)
    registerFunction[FocalMoransI](FocalMoransI.name)(FocalMoransI.apply)
    registerFunction[FocalStdDev](FocalStdDev.name)(FocalStdDev.apply)
    registerFunction[Convolve](Convolve.name)(Convolve.apply)

    registerFunction[Slope](Slope.name)(Slope.apply)
    registerFunction[Aspect](Aspect.name)(Aspect.apply)
    registerFunction[Hillshade](Hillshade.name)(Hillshade.apply)

    registerFunction[MaskByDefined](name = "rf_mask")(MaskByDefined.apply)
    registerFunction[InverseMaskByDefined](name = "rf_inverse_mask")(InverseMaskByDefined.apply)
    registerFunction[MaskByValue](name = "rf_mask_by_value")(MaskByValue.apply)
    registerFunction[InverseMaskByValue](name = "rf_inverse_mask_by_value")(InverseMaskByValue.apply)
    registerFunction[MaskByValues](name = "rf_mask_by_values")(MaskByValues.apply)

    registerFunction[DebugRender.RenderAscii](name = "rf_render_ascii")(DebugRender.RenderAscii.apply)
    registerFunction[DebugRender.RenderMatrix](name = "rf_render_matrix")(DebugRender.RenderMatrix.apply)
    registerFunction[RenderPNG.RenderCompositePNG](name = "rf_render_png")(RenderPNG.RenderCompositePNG.apply)
    registerFunction[RGBComposite](name = "rf_rgb_composite")(RGBComposite.apply)

    registerFunction[XZ2Indexer](name = "rf_xz2_index")(XZ2Indexer(_: Expression, _: Expression, 18.toShort))
    registerFunction[Z2Indexer](name = "rf_z2_index")(Z2Indexer(_: Expression, _: Expression, 31.toShort))

    registerFunction[ReprojectGeometry](name = "st_reproject")(ReprojectGeometry.apply)

    registerFunction[ExtractBits]("rf_local_extract_bits")(ExtractBits.apply)
    registerFunction[ExtractBits]("rf_local_extract_bit")(ExtractBits.apply)
  }
}
