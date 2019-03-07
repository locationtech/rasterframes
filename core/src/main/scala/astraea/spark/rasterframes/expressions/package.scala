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

import astraea.spark.rasterframes.expressions.accessors._
import astraea.spark.rasterframes.expressions.aggstats._
import astraea.spark.rasterframes.expressions.generators._
import astraea.spark.rasterframes.expressions.localops._
import astraea.spark.rasterframes.expressions.tilestats._
import astraea.spark.rasterframes.expressions.transformers._
import geotrellis.raster.{DoubleConstantNoDataCellType, Tile}
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.rf.VersionShims._
import org.apache.spark.sql.{SQLContext, rf}

import scala.util.Try
import scala.reflect.runtime.universe._
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

  /** As opposed to `udf`, this constructs an unwrapped ScalaUDF Expression from a function. */
  private[expressions]
  def udfexpr[RT: TypeTag, A1: TypeTag](name: String, f: A1 => RT): Expression => ScalaUDF = (child: Expression) => {
    val ScalaReflection.Schema(dataType, nullable) = ScalaReflection.schemaFor[RT]
    val inputTypes = Try(ScalaReflection.schemaFor(typeTag[A1]).dataType :: Nil).toOption
    ScalaUDF(f, dataType, Seq(child),  inputTypes.getOrElse(Nil), nullable = nullable, udfName = Some(name))
  }

  def register(sqlContext: SQLContext): Unit = {
    // Expression-oriented functions have a different registration scheme
    // Currently have to register with the `builtin` registry due to Spark data hiding.
    val registry: FunctionRegistry = rf.registry(sqlContext)

    registry.registerExpression[Add]("rf_local_add")
    registry.registerExpression[Subtract]("rf_local_subtract")
    registry.registerExpression[ExplodeTiles]("rf_explode_tiles")
    registry.registerExpression[GetCellType]("rf_cell_type")
    registry.registerExpression[SetCellType]("rf_convert_cell_type")
    registry.registerExpression[GetDimensions]("rf_tile_dimensions")
    registry.registerExpression[BoundsToGeometry]("rf_bounds_geometry")
    registry.registerExpression[Subtract]("rf_local_subtract")
    registry.registerExpression[Multiply]("rf_local_multiply")
    registry.registerExpression[Divide]("rf_local_divide")
    registry.registerExpression[NormalizedDifference]("rf_normalized_difference")
    registry.registerExpression[Less]("rf_local_less")
    registry.registerExpression[Greater]("rf_local_greater")
    registry.registerExpression[LessEqual]("rf_local_less_equal")
    registry.registerExpression[GreaterEqual]("rf_local_greater_equal")
    registry.registerExpression[Equal]("rf_local_equal")
    registry.registerExpression[Unequal]("rf_local_unequal")
    registry.registerExpression[Sum]("rf_tile_sum")
    registry.registerExpression[TileToArrayDouble]("rf_tile_to_array_double")
    registry.registerExpression[TileToArrayInt]("rf_tile_to_array_int")
    registry.registerExpression[DataCells]("rf_data_cells")
    registry.registerExpression[NoDataCells]("rf_no_data_cells")
    registry.registerExpression[IsNoDataTile]("rf_is_no_data_tile")
    registry.registerExpression[TileMin]("rf_tile_min")
    registry.registerExpression[TileMax]("rf_tile_max")
    registry.registerExpression[TileMean]("rf_tile_mean")
    registry.registerExpression[TileStats]("rf_tile_stats")
    registry.registerExpression[TileHistogram]("rf_tile_histogram")
    registry.registerExpression[CellCountAggregate.DataCells]("rf_agg_data_cells")
    registry.registerExpression[CellCountAggregate.NoDataCells]("rf_agg_no_data_cells")
    registry.registerExpression[CellStatsAggregate.CellStatsAggregateUDAF]("rf_agg_stats")
    registry.registerExpression[HistogramAggregate.HistogramAggregateUDAF]("rf_agg_approx_histogram")
    registry.registerExpression[LocalStatsAggregate.LocalStatsAggregateUDAF]("rf_agg_local_stats")
    registry.registerExpression[LocalTileOpAggregate.LocalMinUDAF]("rf_agg_local_min")
    registry.registerExpression[LocalTileOpAggregate.LocalMaxUDAF]("rf_agg_local_max")
    registry.registerExpression[LocalCountAggregate.LocalDataCellsUDAF]("rf_agg_local_data_cells")
    registry.registerExpression[LocalCountAggregate.LocalNoDataCellsUDAF]("rf_agg_local_no_data_cells")
    registry.registerExpression[LocalMeanAggregate]("rf_agg_local_mean")

    registry.registerExpression[Mask.MaskByDefined]("rf_mask")
    registry.registerExpression[Mask.MaskByValue]("rf_mask_by_value")
    registry.registerExpression[Mask.InverseMaskByDefined]("rf_inverse_mask")

    registry.registerExpression[DebugRender.RenderAscii]("rf_render_ascii")
    registry.registerExpression[DebugRender.RenderMatrix]("rf_render_matrix")
  }
}
