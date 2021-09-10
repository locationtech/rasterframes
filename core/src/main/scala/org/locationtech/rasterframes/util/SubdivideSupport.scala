/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea, Inc.
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

package org.locationtech.rasterframes.util

import geotrellis.raster.crop.Crop
import geotrellis.raster.{CellGrid, Dimensions, TileLayout}
import geotrellis.layer._
import geotrellis.util._

/**
 *
 *
 * @since 4/5/18
 */
trait SubdivideSupport {
  implicit class TileLayoutHasSubdivide(self: TileLayout) {
    def subdivide(divs: Int): TileLayout = {
      def shrink(num: Int) = {
        require(num % divs == 0, s"Subdivision of '$divs' does not evenly divide into dimension '$num'")
        num / divs
      }
      def grow(num: Int) = num * divs

      divs match {
        case 0 => self
        case i if i < 0 => throw new IllegalArgumentException(s"divs=$divs must be positive")
        case _ =>
          TileLayout(
            layoutCols = grow(self.layoutCols),
            layoutRows = grow(self.layoutRows),
            tileCols = shrink(self.tileCols),
            tileRows = shrink(self.tileRows)
          )
      }
    }
  }

  implicit class BoundsHasSubdivide[K: SpatialComponent](self: Bounds[K]) {
    def subdivide(divs: Int): Bounds[K] = {
      self.flatMap(kb => {
        val currGrid = kb.toGridBounds()
        // NB: As with GT regrid, we keep the spatial key origin (0, 0) at the same map coordinate
        val newGrid = currGrid.copy(
          colMin = currGrid.colMin * divs,
          rowMin = currGrid.rowMin * divs,
          colMax = currGrid.colMin * divs + (currGrid.width - 1) * divs + 1,
          rowMax = currGrid.rowMin * divs + (currGrid.height - 1) * divs + 1
        )
        kb.setSpatialBounds(KeyBounds(newGrid))
      })
    }
  }

  /**
   * Note: this enrichment makes the assumption that the new keys will be used in the
   * layout regime defined by `TileLayoutHasSubdivide`
   */
  implicit class SpatialKeyHasSubdivide[K: SpatialComponent](self: K) {
    def subdivide(divs: Int): Seq[K] = {
      val base = self.getComponent[SpatialKey]
      val shifted = SpatialKey(base.col * divs, base.row * divs)

      for{
        i <- 0 until divs
        j <- 0 until divs
      } yield {
        val newKey = SpatialKey(shifted.col + j, shifted.row + i)
        self.setComponent(newKey)
      }
    }
  }

  implicit class TileLayerMetadataHasSubdivide[K: SpatialComponent](tlm: TileLayerMetadata[K]) {
    def subdivide(divs: Int): TileLayerMetadata[K] = {
      val tileLayout = tlm.layout.tileLayout.subdivide(divs)
      val layout = tlm.layout.copy(tileLayout = tileLayout)
      val bounds = tlm.bounds.subdivide(divs)
      tlm.copy(layout = layout, bounds = bounds)
    }
  }

  implicit class TileHasSubdivide[T <: CellGrid[Int]: WithCropMethods](self: T) {
    def subdivide(divs: Int): Seq[T] = {
      val Dimensions(cols, rows) = self.dimensions
      val (newCols, newRows) = (cols/divs, rows/divs)
      for {
        i <- 0 until divs
        j <- 0 until divs
      } yield {
        val startCol = j * newCols
        val startRow = i * newRows
        val endCol = startCol + newCols - 1
        val endRow = startRow + newRows - 1
        self.crop(startCol, startRow, endCol, endRow, Crop.Options(force = true))
      }
    }
  }
}

object SubdivideSupport extends SubdivideSupport
