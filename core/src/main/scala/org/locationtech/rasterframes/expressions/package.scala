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
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.rf.VersionShims._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{SQLContext, rf}
import org.locationtech.rasterframes.expressions.accessors._
import org.locationtech.rasterframes.expressions.aggregates.CellCountAggregate.DataCells
import org.locationtech.rasterframes.expressions.aggregates._
import org.locationtech.rasterframes.expressions.generators._
import org.locationtech.rasterframes.expressions.localops._
import org.locationtech.rasterframes.expressions.focalops._
import org.locationtech.rasterframes.expressions.tilestats._
import org.locationtech.rasterframes.expressions.transformers._

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

  def register(sqlContext: SQLContext): Unit = {
    // Expression-oriented functions have a different registration scheme
    // Currently have to register with the `builtin` registry due to Spark data hiding.
    val registry: FunctionRegistry = rf.registry(sqlContext)

    registry.registerExpression[Add]("rf_local_add")
    registry.registerExpression[Subtract]("rf_local_subtract")
    registry.registerExpression[TileAssembler]("rf_assemble_tile")
    registry.registerExpression[ExplodeTiles]("rf_explode_tiles")
    registry.registerExpression[GetCellType]("rf_cell_type")
    registry.registerExpression[SetCellType]("rf_convert_cell_type")
    registry.registerExpression[InterpretAs]("rf_interpret_cell_type_as")
    registry.registerExpression[SetNoDataValue]("rf_with_no_data")
    registry.registerExpression[GetDimensions]("rf_dimensions")
    registry.registerExpression[ExtentToGeometry]("st_geometry")
    registry.registerExpression[GetGeometry]("rf_geometry")
    registry.registerExpression[GeometryToExtent]("st_extent")
    registry.registerExpression[GetExtent]("rf_extent")
    registry.registerExpression[GetCRS]("rf_crs")
    registry.registerExpression[RealizeTile]("rf_tile")
    registry.registerExpression[CreateProjectedRaster]("rf_proj_raster")
    registry.registerExpression[Multiply]("rf_local_multiply")
    registry.registerExpression[Divide]("rf_local_divide")
    registry.registerExpression[NormalizedDifference]("rf_normalized_difference")
    registry.registerExpression[Less]("rf_local_less")
    registry.registerExpression[Greater]("rf_local_greater")
    registry.registerExpression[LessEqual]("rf_local_less_equal")
    registry.registerExpression[GreaterEqual]("rf_local_greater_equal")
    registry.registerExpression[Equal]("rf_local_equal")
    registry.registerExpression[Unequal]("rf_local_unequal")
    registry.registerExpression[IsIn]("rf_local_is_in")
    registry.registerExpression[Undefined]("rf_local_no_data")
    registry.registerExpression[Defined]("rf_local_data")
    registry.registerExpression[Min]("rf_local_min")
    registry.registerExpression[Max]("rf_local_max")
    registry.registerExpression[Clamp]("rf_local_clamp")
    registry.registerExpression[Where]("rf_where")
    registry.registerExpression[Standardize]("rf_standardize")
    registry.registerExpression[Rescale]("rf_rescale")
    registry.registerExpression[Sum]("rf_tile_sum")
    registry.registerExpression[Round]("rf_round")
    registry.registerExpression[Abs]("rf_abs")
    registry.registerExpression[Log]("rf_log")
    registry.registerExpression[Log10]("rf_log10")
    registry.registerExpression[Log2]("rf_log2")
    registry.registerExpression[Log1p]("rf_log1p")
    registry.registerExpression[Exp]("rf_exp")
    registry.registerExpression[Exp10]("rf_exp10")
    registry.registerExpression[Exp2]("rf_exp2")
    registry.registerExpression[ExpM1]("rf_expm1")
    registry.registerExpression[Sqrt]("rf_sqrt")
    registry.registerExpression[Resample]("rf_resample")
    registry.registerExpression[ResampleNearest]("rf_resample_nearest")
    registry.registerExpression[TileToArrayDouble]("rf_tile_to_array_double")
    registry.registerExpression[TileToArrayInt]("rf_tile_to_array_int")
    registry.registerExpression[DataCells]("rf_data_cells")
    registry.registerExpression[NoDataCells]("rf_no_data_cells")
    registry.registerExpression[IsNoDataTile]("rf_is_no_data_tile")
    registry.registerExpression[Exists]("rf_exists")
    registry.registerExpression[ForAll]("rf_for_all")
    registry.registerExpression[TileMin]("rf_tile_min")
    registry.registerExpression[TileMax]("rf_tile_max")
    registry.registerExpression[TileMean]("rf_tile_mean")
    registry.registerExpression[TileStats]("rf_tile_stats")
    registry.registerExpression[TileHistogram]("rf_tile_histogram")
    registry.registerExpression[DataCells]("rf_agg_data_cells")
    registry.registerExpression[CellCountAggregate.NoDataCells]("rf_agg_no_data_cells")
    registry.registerExpression[CellStatsAggregate.CellStatsAggregateUDAF]("rf_agg_stats")
    registry.registerExpression[HistogramAggregate.HistogramAggregateUDAF]("rf_agg_approx_histogram")
    registry.registerExpression[LocalStatsAggregate.LocalStatsAggregateUDAF]("rf_agg_local_stats")
    registry.registerExpression[LocalTileOpAggregate.LocalMinUDAF]("rf_agg_local_min")
    registry.registerExpression[LocalTileOpAggregate.LocalMaxUDAF]("rf_agg_local_max")
    registry.registerExpression[LocalCountAggregate.LocalDataCellsUDAF]("rf_agg_local_data_cells")
    registry.registerExpression[LocalCountAggregate.LocalNoDataCellsUDAF]("rf_agg_local_no_data_cells")
    registry.registerExpression[LocalMeanAggregate]("rf_agg_local_mean")

    registry.registerExpression[FocalMax](FocalMax.name)
    registry.registerExpression[FocalMin](FocalMin.name)
    registry.registerExpression[FocalMean](FocalMean.name)
    registry.registerExpression[FocalMode](FocalMode.name)
    registry.registerExpression[FocalMedian](FocalMedian.name)
    registry.registerExpression[FocalMoransI](FocalMoransI.name)
    registry.registerExpression[FocalStdDev](FocalStdDev.name)
    registry.registerExpression[Convolve](Convolve.name)

    registry.registerExpression[Slope](Slope.name)
    registry.registerExpression[Aspect](Aspect.name)
    registry.registerExpression[Hillshade](Hillshade.name)

    registry.registerExpression[Mask.MaskByDefined]("rf_mask")
    registry.registerExpression[Mask.InverseMaskByDefined]("rf_inverse_mask")
    registry.registerExpression[Mask.MaskByValue]("rf_mask_by_value")
    registry.registerExpression[Mask.InverseMaskByValue]("rf_inverse_mask_by_value")
    registry.registerExpression[Mask.MaskByValues]("rf_mask_by_values")

    registry.registerExpression[DebugRender.RenderAscii]("rf_render_ascii")
    registry.registerExpression[DebugRender.RenderMatrix]("rf_render_matrix")
    registry.registerExpression[RenderPNG.RenderCompositePNG]("rf_render_png")
    registry.registerExpression[RGBComposite]("rf_rgb_composite")

    registry.registerExpression[XZ2Indexer]("rf_xz2_index")
    registry.registerExpression[Z2Indexer]("rf_z2_index")

    registry.registerExpression[transformers.ReprojectGeometry]("st_reproject")

    registry.registerExpression[ExtractBits]("rf_local_extract_bits")
    registry.registerExpression[ExtractBits]("rf_local_extract_bit")
  }
}
