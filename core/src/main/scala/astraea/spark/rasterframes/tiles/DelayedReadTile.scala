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

package astraea.spark.rasterframes.tiles

import astraea.spark.rasterframes.ref.RasterSource
import geotrellis.raster.{CellType, Tile}
import geotrellis.vector.Extent

/**
 * A delayed-read Tile implementation.
 *
 * @since 8/21/18
 */
case class DelayedReadTile(extent: Extent, source: RasterSource) extends DelegatingTile {
  require(source.bandCount == 1, "Expected singleband tile")
  private lazy val realized: Tile = source.read(extent).left.get.tile
  override def cellType: CellType = source.cellType
  override def cols: Int = source.dimensions._1
  override def rows: Int = source.dimensions._2
  protected override def delegate: Tile = realized
}

object DelayedReadTile {
  /** Constructor for when data extent cover whole raster. */
  def apply(source: RasterSource): DelayedReadTile =
    new DelayedReadTile(source.extent, source)
}
