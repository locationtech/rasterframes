/*
 * Copyright 2016 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.locationtech.rasterframes.model


import geotrellis.raster._
import geotrellis.vector._

import scala.math.{ceil, max, min}

/**
  * This class is a copy of the GeoTrellis 2.x `RasterExtent`,
  * with [GT 3.0 fixes](https://github.com/locationtech/geotrellis/pull/2953/files) incorporated into the
  * new `GridExtent[T]` class. This class should be removed after RasterFrames is upgraded to GT 3.x.
  */
case class FixedRasterExtent(
  override val extent: Extent,
  override val cellwidth: Double,
  override val cellheight: Double,
  cols: Int,
  rows: Int
) extends GridExtent(extent, cellwidth, cellheight) with Grid {
  import FixedRasterExtent._

  if (cols <= 0) throw GeoAttrsError(s"invalid cols: $cols")
  if (rows <= 0) throw GeoAttrsError(s"invalid rows: $rows")

  /**
    * Convert map coordinates (x, y) to grid coordinates (col, row).
    */
  final def mapToGrid(x: Double, y: Double): (Int, Int) = {
    val col = floorWithTolerance((x - extent.xmin) / cellwidth).toInt
    val row = floorWithTolerance((extent.ymax - y) / cellheight).toInt
    (col, row)
  }

  /**
    * Convert map coordinate x to grid coordinate column.
    */
  final def mapXToGrid(x: Double): Int = floorWithTolerance(mapXToGridDouble(x)).toInt

  /**
    * Convert map coordinate x to grid coordinate column.
    */
  final def mapXToGridDouble(x: Double): Double = (x - extent.xmin) / cellwidth

  /**
    * Convert map coordinate y to grid coordinate row.
    */
  final def mapYToGrid(y: Double): Int = floorWithTolerance(mapYToGridDouble(y)).toInt

  /**
    * Convert map coordinate y to grid coordinate row.
    */
  final def mapYToGridDouble(y: Double): Double = (extent.ymax - y ) / cellheight

  /**
    * Convert map coordinate tuple (x, y) to grid coordinates (col, row).
    */
  final def mapToGrid(mapCoord: (Double, Double)): (Int, Int) = {
    val (x, y) = mapCoord
    mapToGrid(x, y)
  }

  /**
    * Convert a point to grid coordinates (col, row).
    */
  final def mapToGrid(p: Point): (Int, Int) =
    mapToGrid(p.x, p.y)

  /**
    * The map coordinate of a grid cell is the center point.
    */
  final def gridToMap(col: Int, row: Int): (Double, Double) = {
    val x = col * cellwidth + extent.xmin + (cellwidth / 2)
    val y = extent.ymax - (row * cellheight) - (cellheight / 2)

    (x, y)
  }

  /**
    * For a give column, find the corresponding x-coordinate in the
    * grid of the present [[FixedRasterExtent]].
    */
  final def gridColToMap(col: Int): Double = {
    col * cellwidth + extent.xmin + (cellwidth / 2)
  }

  /**
    * For a give row, find the corresponding y-coordinate in the grid
    * of the present [[FixedRasterExtent]].
    */
  final def gridRowToMap(row: Int): Double = {
    extent.ymax - (row * cellheight) - (cellheight / 2)
  }

  /**
    * Gets the GridBounds aligned with this FixedRasterExtent that is the
    * smallest subgrid of containing all points within the extent. The
    * extent is considered inclusive on it's north and west borders,
    * exclusive on it's east and south borders.  See [[FixedRasterExtent]]
    * for a discussion of grid and extent boundary concepts.
    *
    * The 'clamp' flag determines whether or not to clamp the
    * GridBounds to the FixedRasterExtent; defaults to true. If false,
    * GridBounds can contain negative values, or values outside of
    * this FixedRasterExtent's boundaries.
    *
    * @param     subExtent      The extent to get the grid bounds for
    * @param     clamp          A boolean
    */
  def gridBoundsFor(subExtent: Extent, clamp: Boolean = true): GridBounds = {
    // West and North boundaries are a simple mapToGrid call.
    val (colMin, rowMin) = mapToGrid(subExtent.xmin, subExtent.ymax)

    // If South East corner is on grid border lines, we want to still only include
    // what is to the West and\or North of the point. However if the border point
    // is not directly on a grid division, include the whole row and/or column that
    // contains the point.
    val colMax = {
      val colMaxDouble = mapXToGridDouble(subExtent.xmax)
      if(math.abs(colMaxDouble - floorWithTolerance(colMaxDouble)) < FixedRasterExtent.epsilon) colMaxDouble.toInt - 1
      else colMaxDouble.toInt
    }

    val rowMax = {
      val rowMaxDouble = mapYToGridDouble(subExtent.ymin)
      if(math.abs(rowMaxDouble - floorWithTolerance(rowMaxDouble)) < FixedRasterExtent.epsilon) rowMaxDouble.toInt - 1
      else rowMaxDouble.toInt
    }

    if(clamp) {
      GridBounds(math.min(math.max(colMin, 0), cols - 1),
        math.min(math.max(rowMin, 0), rows - 1),
        math.min(math.max(colMax, 0), cols - 1),
        math.min(math.max(rowMax, 0), rows - 1))
    } else {
      GridBounds(colMin, rowMin, colMax, rowMax)
    }
  }

  /**
    * Combine two different [[FixedRasterExtent]]s (which must have the
    * same cellsizes).  The result is a new extent at the same
    * resolution.
    */
  def combine (that: FixedRasterExtent): FixedRasterExtent = {
    if (cellwidth != that.cellwidth)
      throw GeoAttrsError(s"illegal cellwidths: $cellwidth and ${that.cellwidth}")
    if (cellheight != that.cellheight)
      throw GeoAttrsError(s"illegal cellheights: $cellheight and ${that.cellheight}")

    val newExtent = extent.combine(that.extent)
    val newRows = ceil(newExtent.height / cellheight).toInt
    val newCols = ceil(newExtent.width / cellwidth).toInt

    FixedRasterExtent(newExtent, cellwidth, cellheight, newCols, newRows)
  }

  /**
    * Returns a [[RasterExtent]] with the same extent, but a modified
    * number of columns and rows based on the given cell height and
    * width.
    */
  def withResolution(targetCellWidth: Double, targetCellHeight: Double): FixedRasterExtent = {
    val newCols = math.ceil((extent.xmax - extent.xmin) / targetCellWidth).toInt
    val newRows = math.ceil((extent.ymax - extent.ymin) / targetCellHeight).toInt
    FixedRasterExtent(extent, targetCellWidth, targetCellHeight, newCols, newRows)
  }

  /**
    * Returns a [[FixedRasterExtent]] with the same extent, but a modified
    * number of columns and rows based on the given cell height and
    * width.
    */
  def withResolution(cellSize: CellSize): FixedRasterExtent =
    withResolution(cellSize.width, cellSize.height)

  /**
    * Returns a [[FixedRasterExtent]] with the same extent and the given
    * number of columns and rows.
    */
  def withDimensions(targetCols: Int, targetRows: Int): FixedRasterExtent =
    FixedRasterExtent(extent, targetCols, targetRows)

  /**
    * Adjusts a raster extent so that it can encompass the tile
    * layout.  Will resample the extent, but keep the resolution, and
    * preserve north and west borders
    */
  def adjustTo(tileLayout: TileLayout): FixedRasterExtent = {
    val totalCols = tileLayout.tileCols * tileLayout.layoutCols
    val totalRows = tileLayout.tileRows * tileLayout.layoutRows

    val resampledExtent = Extent(extent.xmin, extent.ymax - (cellheight*totalRows),
      extent.xmin + (cellwidth*totalCols), extent.ymax)

    FixedRasterExtent(resampledExtent, cellwidth, cellheight, totalCols, totalRows)
  }

  /**
    * Returns a new [[FixedRasterExtent]] which represents the GridBounds
    * in relation to this FixedRasterExtent.
    */
  def rasterExtentFor(gridBounds: GridBounds): FixedRasterExtent = {
    val (xminCenter, ymaxCenter) = gridToMap(gridBounds.colMin, gridBounds.rowMin)
    val (xmaxCenter, yminCenter) = gridToMap(gridBounds.colMax, gridBounds.rowMax)
    val (hcw, hch) = (cellwidth / 2, cellheight / 2)
    val e = Extent(xminCenter - hcw, yminCenter - hch, xmaxCenter + hcw, ymaxCenter + hch)
    FixedRasterExtent(e, cellwidth, cellheight, gridBounds.width, gridBounds.height)
  }
}

/**
  * The companion object for the [[FixedRasterExtent]] type.
  */
object FixedRasterExtent {
  final val epsilon = 0.0000001

  /**
    * Create a new [[FixedRasterExtent]] from an Extent, a column, and a
    * row.
    */
  def apply(extent: Extent, cols: Int, rows: Int): FixedRasterExtent = {
    val cw = extent.width / cols
    val ch = extent.height / rows
    FixedRasterExtent(extent, cw, ch, cols, rows)
  }

  /**
    * Create a new [[FixedRasterExtent]] from an Extent and a [[CellSize]].
    */
  def apply(extent: Extent, cellSize: CellSize): FixedRasterExtent = {
    val cols = (extent.width / cellSize.width).toInt
    val rows = (extent.height / cellSize.height).toInt
    FixedRasterExtent(extent, cellSize.width, cellSize.height, cols, rows)
  }

  /**
    * Create a new [[FixedRasterExtent]] from a [[CellGrid]] and an Extent.
    */
  def apply(tile: CellGrid, extent: Extent): FixedRasterExtent =
    apply(extent, tile.cols, tile.rows)

  /**
    * Create a new [[FixedRasterExtent]] from an Extent and a [[CellGrid]].
    */
  def apply(extent: Extent, tile: CellGrid): FixedRasterExtent =
    apply(extent, tile.cols, tile.rows)


  /**
    * The same logic is used in QGIS: https://github.com/qgis/QGIS/blob/607664c5a6b47c559ed39892e736322b64b3faa4/src/analysis/raster/qgsalignraster.cpp#L38
    * The search query: https://github.com/qgis/QGIS/search?p=2&q=floor&type=&utf8=%E2%9C%93
    *
    * GDAL uses smth like that, however it was a bit hard to track it down:
    * https://github.com/OSGeo/gdal/blob/7601a637dfd204948d00f4691c08f02eb7584de5/gdal/frmts/vrt/vrtsources.cpp#L215
    * */
  def floorWithTolerance(value: Double): Double = {
    val roundedValue = math.round(value)
    if (math.abs(value - roundedValue) < epsilon) roundedValue
    else math.floor(value)
  }
}

