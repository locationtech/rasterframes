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

package org.locationtech.rasterframes.ref

import com.typesafe.scalalogging.Logger
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.Tags
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.{CellType, GridBounds, MultibandTile, Raster}
import geotrellis.util.RangeReader
import geotrellis.vector.Extent
import org.locationtech.rasterframes.util.GeoTiffInfoSupport
import org.slf4j.LoggerFactory

trait RangeReaderRasterSource extends RasterSource with GeoTiffInfoSupport {
  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  protected def rangeReader: RangeReader

  private def realInfo =
    GeoTiffReader.readGeoTiffInfo(rangeReader, streaming = true, withOverviews = false)

  protected lazy val tiffInfo = SimpleRasterInfo(realInfo)

  def crs: CRS = tiffInfo.crs

  def extent: Extent = tiffInfo.extent

  override def cols: Int = tiffInfo.rasterExtent.cols

  override def rows: Int = tiffInfo.rasterExtent.rows

  def cellType: CellType = tiffInfo.cellType

  def bandCount: Int = tiffInfo.bandCount

  override def tags: Tags = tiffInfo.tags

  override protected def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    val info = realInfo
    val geoTiffTile = GeoTiffReader.geoTiffMultibandTile(info)
    val intersectingBounds = bounds.flatMap(_.intersection(this)).toSeq
    geoTiffTile.crop(intersectingBounds, bands.toArray).map {
      case (gb, tile) =>
        Raster(tile, rasterExtent.extentFor(gb, clamp = true))
    }
  }
}
