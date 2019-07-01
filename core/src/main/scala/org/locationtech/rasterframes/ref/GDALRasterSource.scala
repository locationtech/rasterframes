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

import java.net.URI

import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, GridBounds, MultibandTile, Raster}
import geotrellis.raster.io.geotiff.Tags
import geotrellis.vector.Extent
import org.locationtech.rasterframes.ref.RasterSource.URIRasterSource
import geotrellis.contrib.vlm.gdal.{GDALRasterSource => VLMRasterSource}

case class GDALRasterSource(source: URI) extends RasterSource with URIRasterSource {

  @transient
  private lazy val gdal: VLMRasterSource = {
    val cleaned = source.toASCIIString.replace("gdal+", "")
    // VSIPath doesn't like single slash "file:/path..."
    val tweaked =
      if (cleaned.matches("^file:/[^/].*"))
        cleaned.replace("file:", "")
      else cleaned

    VLMRasterSource(tweaked)
  }

  protected def tiffInfo = SimpleRasterInfo.cache.get(source.toASCIIString, _ => SimpleRasterInfo(gdal))

  override def crs: CRS = tiffInfo.crs

  override def extent: Extent = tiffInfo.extent

  private def metadata = Map.empty[String, String]

  override def cellType: CellType = tiffInfo.cellType

  override def bandCount: Int = tiffInfo.bandCount

  override def cols: Int = tiffInfo.cols

  override def rows: Int = tiffInfo.rows

  override def tags: Tags = Tags(metadata, List.empty)

  override protected def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
    gdal.readBounds(bounds, bands)
}
