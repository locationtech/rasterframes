/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2019 Astraea, Inc.
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

package org.locationtech.rasterframes.expressions.aggregates

import org.locationtech.rasterframes._
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import geotrellis.proj4.CRS
import geotrellis.raster.resample.ResampleMethod
import geotrellis.raster.{ArrayTile, CellType, MutableArrayTile, Raster, Tile}
import geotrellis.vector.Extent
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{Column, Row, TypedColumn}
import geotrellis.raster.reproject.Reproject
import org.locationtech.rasterframes.expressions.aggregates.TileRasterizerAggregate.ProjectedRasterDefinition

/**
  * Aggregation function for creating a single raster from tiles.
  * @param prd settings
  */
class TileRasterizerAggregate(prd: ProjectedRasterDefinition) extends UserDefinedAggregateFunction {

  val projOpts = Reproject.Options.DEFAULT.copy(method = prd.sampler)

  override def deterministic: Boolean = true

  override def inputSchema: StructType = StructType(Seq(
    StructField("crs", schemaOf[CRS], false),
    StructField("extent", schemaOf[Extent], false),
    StructField("tile", TileType)
  ))

  override def bufferSchema: StructType = StructType(Seq(
    StructField("tile_buffer", TileType),
    StructField("extent_buffer", schemaOf[Extent])
  ))

  override def dataType: DataType = schemaOf[Raster[Tile]]

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ArrayTile.empty(prd.cellType, prd.cols, prd.rows)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val crs = input.getAs[Row](0).to[CRS]
    val extent = input.getAs[Row](1).to[Extent]

    val localExtent = extent.reproject(crs, prd.crs)

    if (prd.extent.intersects(localExtent)) {
      val localTile = input.getAs[Tile](2).reproject(extent, crs, prd.crs, projOpts)
      val bt = buffer.getAs[MutableArrayTile](0)
      val merged = bt.merge(prd.extent, localExtent, localTile.tile, prd.sampler)
      buffer(0) = merged
      if (buffer.isNullAt(1))
        buffer(1) = localExtent.toRow
      else {
        val curr = buffer.getAs[Row](1).to[Extent]
        buffer(1) = curr.combine(localExtent).toRow
      }
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val leftTile = buffer1.getAs[MutableArrayTile](0)
    val rightTile = buffer2.getAs[MutableArrayTile](0)
    val leftExtent = buffer1.getAs[Row](1).to[Extent]
    val rightExtent = buffer2.getAs[Row](1).to[Extent]
    buffer1(0) = leftTile.merge(rightTile)
    buffer1(1) =
      if (leftExtent == null) rightExtent
      else if (rightExtent == null) leftExtent
      else leftExtent.combine(rightExtent)
  }

  override def evaluate(buffer: Row): Raster[Tile] = {
    val t = buffer.getAs[Tile](0)
    val e = buffer.getAs[Row](1).to[Extent]
    Raster(t, e)
  }
}

object TileRasterizerAggregate {
  val nodeName = "tile_rasterizer_aggregate"
  /**  Convenience grouping of  parameters needed for running aggregate. */
  case class ProjectedRasterDefinition(cols: Int, rows: Int, cellType: CellType, crs: CRS, extent: Extent, sampler: ResampleMethod = ResampleMethod.DEFAULT)

  def apply(prd: ProjectedRasterDefinition, crsCol: Column, extentCol: Column, tileCol: Column): TypedColumn[Any, Raster[Tile]] =
    new TileRasterizerAggregate(prd)(crsCol, extentCol, tileCol).as(nodeName).as[Raster[Tile]]
}