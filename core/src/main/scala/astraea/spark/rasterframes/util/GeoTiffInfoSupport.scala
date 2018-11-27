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

package astraea.spark.rasterframes.util
import geotrellis.raster.TileLayout
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff.reader.GeoTiffReader.GeoTiffInfo
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.spark.{KeyBounds, SpatialKey, TileLayerMetadata}
import geotrellis.util.ByteReader

/**
 * Utility mix-in for generating a tlm from GeoTiff headers.
 *
 * @since 5/4/18
 */
trait GeoTiffInfoSupport {

  val MAX_SIZE = 256
  private def defaultLayout(cols: Int, rows: Int): TileLayout = {
    def divs(cells: Int) = {
      val layoutDivs = math.ceil(cells / MAX_SIZE.toFloat)
      val tileDivs = math.ceil(cells / layoutDivs)
      (layoutDivs.toInt, tileDivs.toInt)
    }
    val (layoutCols, tileCols) = divs(cols)
    val (layoutRows, tileRows) = divs(rows)
    TileLayout(layoutCols, layoutRows, tileCols, tileRows)
  }

  def extractGeoTiffLayout(reader: ByteReader): (GeoTiffReader.GeoTiffInfo, TileLayerMetadata[SpatialKey]) = {
    val info: GeoTiffInfo = Shims.readGeoTiffInfo(reader, false, true)
    (info, extractGeoTiffLayout(info))
  }

  def extractGeoTiffLayout(info: GeoTiffInfo): TileLayerMetadata[SpatialKey] = {
    // Some notes on GeoTiffInfo properties:
    // * `info.extent` is the actual geotiff extent
    // * `info.segmentLayout.tileLayout` contains the internal, regularized gridding of a tiled GeoTIFF
    // * `info.segmentLayout.tileLayout.{totalCols|totalRows}` is the largest number of possible cells in the internal gridding
    // * `info.segmentLayout.{totalCols|totalRows}` are the real dimensions of the GeoTIFF. This is likely smaller than
    //     the total size of `info.segmentLayout.tileLayout.{totalCols|totalRows}`
    // * `info.rasterExtent.{cellwidth|cellheight}` is the per-pixel spatial resolution
    // * `info.extent` and `info.rasterExtent.extent` are the same thing

    val tlm = {
      val tileLayout = if(info.segmentLayout.isTiled) {
        info.segmentLayout.tileLayout
      }
      else {
        val width = info.segmentLayout.totalCols
        val height = info.segmentLayout.totalRows
        defaultLayout(width, height)
      }
      val extent = info.extent
      val crs = info.crs
      val cellType = info.cellType
      val bounds = KeyBounds(
        SpatialKey(0, 0),
        SpatialKey(tileLayout.layoutCols - 1, tileLayout.layoutRows - 1)
      )

      TileLayerMetadata(cellType,
        LayoutDefinition(info.rasterExtent, tileLayout.tileCols, tileLayout.tileRows),
        extent, crs, bounds)
    }

    tlm
  }
}
