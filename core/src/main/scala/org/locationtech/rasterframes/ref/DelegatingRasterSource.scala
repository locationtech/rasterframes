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

import geotrellis.raster.{RasterSource => GTRasterSource}
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.Tags
import geotrellis.raster.{CellType, GridBounds, MultibandTile, Raster}
import geotrellis.vector.Extent
import org.locationtech.rasterframes.ref.RFRasterSource.URIRasterSource

/** A RasterFrames RasterSource which delegates most operations to a geotrellis-contrib RasterSource */
abstract class DelegatingRasterSource(source: URI, delegateBuilder: () => GTRasterSource) extends RFRasterSource with URIRasterSource {
  @transient
  @volatile
  private var _delRef: GTRasterSource = _

  private def retryableRead[R >: Null](f: GTRasterSource => R): R = synchronized {
    try {
      if (_delRef == null)
        _delRef = delegateBuilder()
      f(_delRef)
    }
    catch {
      // On this Exeception we attempt to recreate the delegate and read again.
      case _: java.nio.BufferUnderflowException =>
        _delRef = null
        val newDel = delegateBuilder()
        val result = f(newDel)
        _delRef = newDel
        result
    }
  }

  // Bad?
  override def equals(obj: Any): Boolean = obj match {
    case drs: DelegatingRasterSource => drs.source == source
    case _                           => false
  }

  override def hashCode(): Int = source.hashCode()

  // This helps reduce header reads between serializations
  def info: SimpleRasterInfo = SimpleRasterInfo(source.toASCIIString, _ =>
    retryableRead(rs => SimpleRasterInfo(rs))
  )

  def cols: Int = info.cols.toInt
  def rows: Int = info.rows.toInt
  def crs: CRS = info.crs
  def extent: Extent = info.extent
  def cellType: CellType = info.cellType
  def bandCount: Int = info.bandCount
  def tags: Tags = info.tags

  def readBounds(bounds: Traversable[GridBounds[Int]], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
    retryableRead(_.readBounds(bounds.map(_.toGridType[Long]), bands))

  override def read(bounds: GridBounds[Int], bands: Seq[Int]): Raster[MultibandTile] =
    retryableRead(_.read(bounds.toGridType[Long], bands)
      .getOrElse(throw new IllegalArgumentException(s"Bounds '$bounds' outside of source"))
    )

  override def read(extent: Extent, bands: Seq[Int]): Raster[MultibandTile] =
    retryableRead(_.read(extent, bands)
      .getOrElse(throw new IllegalArgumentException(s"Extent '$extent' outside of source"))
    )
}
