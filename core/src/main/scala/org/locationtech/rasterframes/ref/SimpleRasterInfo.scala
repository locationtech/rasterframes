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

import com.github.blemale.scaffeine.Scaffeine
import geotrellis.proj4.CRS
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff.Tags
import geotrellis.raster.io.geotiff.reader.GeoTiffInfo
import geotrellis.raster.{CellType, RasterExtent, RasterSource => GTRasterSource}
import geotrellis.vector.Extent
import org.locationtech.rasterframes.ref.RFRasterSource.EMPTY_TAGS

case class SimpleRasterInfo(
  cols: Long,
  rows: Long,
  cellType: CellType,
  extent: Extent,
  rasterExtent: RasterExtent,
  crs: CRS,
  tags: Tags,
  bandCount: Int,
  noDataValue: Option[Double]
)

object SimpleRasterInfo {
  // Not a fan of this.... need a better abstraction that doesn't put the
  // type-specific logic in here.
  def apply(key: String, builder: String => SimpleRasterInfo): SimpleRasterInfo =
    cache.get(key, builder)

  def apply(info: GeoTiffInfo): SimpleRasterInfo =
    SimpleRasterInfo(
      info.segmentLayout.totalCols,
      info.segmentLayout.totalRows,
      info.cellType,
      info.extent,
      info.rasterExtent,
      info.crs,
      info.tags,
      info.bandCount,
      info.noDataValue
    )

  def apply(rs: GTRasterSource): SimpleRasterInfo = {
    def fetchTags: Tags = rs match {
      case gt: GeoTiffRasterSource => gt.tiff.tags
      case _                       => EMPTY_TAGS
    }

    new SimpleRasterInfo(
      rs.cols,
      rs.rows,
      rs.cellType,
      rs.extent,
      rs.gridExtent.toRasterExtent(),
      rs.crs,
      fetchTags,
      rs.bandCount,
      None
    )
  }

  private lazy val cache =
    Scaffeine()
      .recordStats()
      .build[String, SimpleRasterInfo]

  def cacheStats = cache.stats()
}
