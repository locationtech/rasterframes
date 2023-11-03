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

import java.io.IOException
import java.net.URI

import com.azavea.gdal.GDALWarp
import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.CRS
import geotrellis.raster.gdal.{GDALRasterSource => VLMRasterSource}
import geotrellis.raster.io.geotiff.Tags
import geotrellis.raster.{CellType, GridBounds, MultibandTile, Raster}
import geotrellis.vector.Extent
import org.locationtech.rasterframes.ref.RFRasterSource.URIRasterSource


case class GDALRasterSource(source: URI) extends RFRasterSource with URIRasterSource {

  @transient
  protected lazy val gdal: VLMRasterSource = {
    val cleaned = source.toASCIIString
      .replace("gdal+", "")
      .replace("gdal:/", "")
    // VSIPath doesn't like single slash "file:/path..."
    // Local windows path regex used in VSIPath incorrectly removes only 1 slash of scheme
    val tweaked =
      if (cleaned.matches("^file:/[^/].*"))
        cleaned.replaceFirst("^file:/", "file://")
      else cleaned

    VLMRasterSource(cleaned) // temporary work around to use `cleaned` not `tweaked`
  }

  protected def tiffInfo = SimpleRasterInfo(source.toASCIIString, _ => SimpleRasterInfo(gdal))

  def crs: CRS = tiffInfo.crs

  def extent: Extent = tiffInfo.extent

  def metadata = Map.empty[String, String]

  def cellType: CellType = tiffInfo.cellType

  def bandCount: Int = tiffInfo.bandCount

  def cols: Int = tiffInfo.cols.toInt

  def rows: Int = tiffInfo.rows.toInt

  def tags: Tags = Tags(metadata, List.empty)

  def readBounds(bounds: Traversable[GridBounds[Int]], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
    try {
      gdal.readBounds(bounds.map(_.toGridType[Long]), bands)
    } catch {
      case e: Exception => throw new IOException(s"Error reading '$source'", e)
    }
}

object GDALRasterSource extends LazyLogging {
  def gdalVersion(): String = if (hasGDAL) GDALWarp.get_version_info("--version").trim else "not available"

  @transient
  lazy val hasGDAL: Boolean = try {
    val _ = new GDALWarp()
    true
  } catch {
    case _: UnsatisfiedLinkError =>
      logger.warn("GDAL native bindings are not available. Falling back to JVM-based reader for GeoTIFF format.")
      false
  }
}
