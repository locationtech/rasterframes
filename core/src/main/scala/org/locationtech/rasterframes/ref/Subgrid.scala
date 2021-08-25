package org.locationtech.rasterframes.ref

import geotrellis.raster.GridBounds

case class Subgrid(colMin: Int, rowMin: Int, colMax: Int, rowMax: Int) {
    def toGridBounds: GridBounds[Int] =
    GridBounds(colMin, rowMin, colMax, rowMax)
}

object Subgrid {
  def apply(grid: GridBounds[Int]): Subgrid =
    Subgrid(grid.colMin, grid.rowMin, grid.colMax, grid.rowMax)
}