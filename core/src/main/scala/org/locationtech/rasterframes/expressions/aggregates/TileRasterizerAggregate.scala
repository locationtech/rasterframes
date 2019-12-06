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

import geotrellis.proj4.CRS
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.resample.{Bilinear, ResampleMethod}
import geotrellis.raster.{ArrayTile, CellType, MultibandTile, ProjectedRaster, Tile}
import geotrellis.spark.{SpatialKey, TileLayerMetadata}
import geotrellis.vector.Extent
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, TypedColumn}
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.expressions.aggregates.TileRasterizerAggregate.ProjectedRasterDefinition
import org.locationtech.rasterframes.model.TileDimensions
import org.locationtech.rasterframes.util._
import org.slf4j.LoggerFactory

/**
  * Aggregation function for creating a single `geotrellis.raster.Raster[Tile]` from
  * `Tile`, `CRS` and `Extent` columns.
  * @param prd aggregation settings
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
    StructField("tile_buffer", TileType)
  ))

  override def dataType: DataType = TileType

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ArrayTile.empty(prd.destinationCellType, prd.totalCols, prd.totalRows)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val crs = input.getAs[Row](0).to[CRS]
    val extent = input.getAs[Row](1).to[Extent]

    val localExtent = extent.reproject(crs, prd.destinationCRS)

    if (prd.destinationExtent.intersects(localExtent)) {
      val localTile = input.getAs[Tile](2).reproject(extent, crs, prd.destinationCRS, projOpts)
      val bt = buffer.getAs[Tile](0)
      val merged = bt.merge(prd.destinationExtent, localExtent, localTile.tile, prd.sampler)
      buffer(0) = merged
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val leftTile = buffer1.getAs[Tile](0)
    val rightTile = buffer2.getAs[Tile](0)
    buffer1(0) = leftTile.merge(rightTile)
  }

  override def evaluate(buffer: Row): Tile = buffer.getAs[Tile](0)
}

object TileRasterizerAggregate {
  @transient
  private lazy val logger = LoggerFactory.getLogger(getClass)

  /** Convenience grouping of  parameters needed for running aggregate. */
  case class ProjectedRasterDefinition(totalCols: Int, totalRows: Int, destinationCellType: CellType, destinationCRS: CRS,
    destinationExtent: Extent, sampler: ResampleMethod)

  object ProjectedRasterDefinition {
    def apply(tlm: TileLayerMetadata[_], sampler: ResampleMethod): ProjectedRasterDefinition = {
      // Try to determine the actual dimensions of our data coverage
      val TileDimensions(cols, rows) = tlm.totalDimensions
      new ProjectedRasterDefinition(cols, rows, tlm.cellType, tlm.crs, tlm.extent, sampler)
    }
  }

  def apply(prd: ProjectedRasterDefinition, crsCol: Column, extentCol: Column, tileCol: Column): TypedColumn[Any, Tile] = {
    if (prd.totalCols.toDouble * prd.totalRows * 64.0 > Runtime.getRuntime.totalMemory() * 0.5)
      logger.warn(
        s"You've asked for the construction of a very large image (${prd.totalCols} x ${prd.totalRows}). Out of memory error likely.")

    new TileRasterizerAggregate(prd)(crsCol, extentCol, tileCol)
      .as("rf_agg_overview_raster")
      .as[Tile]
  }

  /** Extract a multiband raster from all tile columns. */
  def collect(df: DataFrame, destCRS: CRS, destExtent: Option[Extent], rasterDims: Option[TileDimensions]): ProjectedRaster[MultibandTile] = {
    val tileCols = WithDataFrameMethods(df).tileColumns
    require(tileCols.nonEmpty, "need at least one tile column")
    // Select the anchoring Tile, Extent and CRS columns
    val (extCol, crsCol, tileCol) = {
      // Favor "ProjectedRaster" columns
      val prCols = df.projRasterColumns
      if (prCols.nonEmpty) {
        (rf_extent(prCols.head), rf_crs(prCols.head), rf_tile(prCols.head))
      } else {
        // If no "ProjectedRaster" column, look for single Extent and CRS columns.
        val crsCols = df.crsColumns
        require(crsCols.size == 1, "Exactly one CRS column must be in DataFrame")
        val extentCols = df.extentColumns
        require(extentCols.size == 1, "Exactly one Extent column must be in DataFrame")
        (extentCols.head, crsCols.head, tileCols.head)
      }
    }

    // Scan table and construct what the TileLayerMetadata would be in the specified destination CRS.
    val tlm: TileLayerMetadata[SpatialKey] = df
      .select(
        ProjectedLayerMetadataAggregate(
          destCRS,
          extCol,
          crsCol,
          rf_cell_type(tileCol),
          rf_dimensions(tileCol)
        ))
      .first()
    logger.debug(s"Collected TileLayerMetadata: ${tlm.toString}")

    val c = ProjectedRasterDefinition(tlm, Bilinear)

    val config = rasterDims
      .map { dims =>
        c.copy(totalCols = dims.cols, totalRows = dims.rows)
      }
      .getOrElse(c)

    destExtent.map { ext =>
      c.copy(destinationExtent = ext)
    }

    val aggs = tileCols
      .map(t => TileRasterizerAggregate(config, crsCol, extCol, rf_tile(t))("tile").as(t.columnName))

    val agg = df.select(aggs: _*)

    val row = agg.first()

    val bands = for (i <- 0 until row.size) yield row.getAs[Tile](i)

    ProjectedRaster(MultibandTile(bands), tlm.extent, tlm.crs)
  }
}