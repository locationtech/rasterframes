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

import geotrellis.layer._
import geotrellis.proj4.CRS
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.resample.{Bilinear, ResampleMethod}
import geotrellis.raster.{ArrayTile, CellType, Dimensions, MultibandTile, MutableArrayTile, ProjectedRaster, Tile}
import geotrellis.vector.Extent
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions.udaf
import org.apache.spark.sql.{Column, DataFrame, Encoder, TypedColumn}
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.encoders.StandardEncoders
import org.locationtech.rasterframes.expressions.aggregates.TileRasterizerAggregate.ProjectedRasterDefinition
import org.locationtech.rasterframes.tiles.ProjectedRasterTile
import org.locationtech.rasterframes.util._
import org.slf4j.LoggerFactory

/**
  * Aggregation function for creating a single `geotrellis.raster.Raster[Tile]` from
  * `Tile`, `CRS` and `Extent` columns.
  * @param prd aggregation settings
  */
class TileRasterizerAggregate(prd: ProjectedRasterDefinition) extends Aggregator[ProjectedRasterTile, Tile, Tile] {
  val projOpts = Reproject.Options.DEFAULT.copy(method = prd.sampler)

  override def zero: MutableArrayTile = ArrayTile.empty(prd.destinationCellType, prd.totalCols, prd.totalRows)

  override def reduce(b: Tile, a: ProjectedRasterTile): Tile = {
    val localExtent = a.extent.reproject(a.crs, prd.destinationCRS)
    if (prd.destinationExtent.intersects(localExtent)) {
      val localTile = a.tile.reproject(a.extent, a.crs, prd.destinationCRS, projOpts)
      b.merge(prd.destinationExtent, localExtent, localTile.tile, prd.sampler)
    } else b
  }

  override def merge(b1: Tile, b2: Tile): Tile = b1.merge(b2)

  override def finish(reduction: Tile): Tile = reduction

  override def bufferEncoder: Encoder[Tile] = StandardEncoders.tileEncoder

  override def outputEncoder: Encoder[Tile] = StandardEncoders.tileEncoder
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
      val Dimensions(cols, rows) = tlm.totalDimensions
      require(cols <= Int.MaxValue && rows <= Int.MaxValue, s"Can't construct a Raster of size $cols x $rows. (Too big!)")
      new ProjectedRasterDefinition(cols.toInt, rows.toInt, tlm.cellType, tlm.crs, tlm.extent, sampler)
    }
  }

  def apply(prd: ProjectedRasterDefinition, crsCol: Column, extentCol: Column, tileCol: Column): TypedColumn[Any, Tile] = {
    if (prd.totalCols.toDouble * prd.totalRows * 64.0 > Runtime.getRuntime.totalMemory() * 0.5)
      logger.warn(
        s"You've asked for the construction of a very large image (${prd.totalCols} x ${prd.totalRows}). Out of memory error likely.")

    udaf(new TileRasterizerAggregate(prd))
      .apply(crsCol, extentCol, tileCol)
      .as("rf_agg_overview_raster")
      .as[Tile]
  }

  /** Extract a multiband raster from all tile columns. */
  def collect(df: DataFrame, destCRS: CRS, destExtent: Option[Extent], rasterDims: Option[Dimensions[Int]]): ProjectedRaster[MultibandTile] = {
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
    val tlm: TileLayerMetadata[SpatialKey] =
      df
        .select(
          ProjectedLayerMetadataAggregate(
            destCRS,
            extCol,
            rf_crs(crsCol),
            rf_cell_type(tileCol),
            rf_dimensions(tileCol)
          )
        )
        .first()

    logger.debug(s"Collected TileLayerMetadata: ${tlm.toString}")

    val c = ProjectedRasterDefinition(tlm, Bilinear)

    val config =
      rasterDims
        .map { dims => c.copy(totalCols = dims.cols, totalRows = dims.rows) }
        .getOrElse(c)

    destExtent.map { ext => c.copy(destinationExtent = ext) }

    val aggs = tileCols.map(t => TileRasterizerAggregate(config, rf_crs(crsCol), extCol, rf_tile(t)).as(t.columnName))

    val agg = df.select(aggs: _*)

    val row = agg.first()

    val bands = for (i <- 0 until row.size) yield row.getAs[Tile](i)

    ProjectedRaster(MultibandTile(bands), tlm.extent, tlm.crs)
  }
}