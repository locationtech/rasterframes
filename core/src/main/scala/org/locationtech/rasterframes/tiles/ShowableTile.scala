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

package org.locationtech.rasterframes.tiles

import geotrellis.raster.{DelegatingTile, Tile, isNoData}
import org.locationtech.rasterframes._

class ShowableTile(val delegate: Tile) extends DelegatingTile {
  override def equals(obj: Any): Boolean = obj match {
    case st: ShowableTile => delegate.equals(st.delegate)
    case o => delegate.equals(o)
  }
  override def hashCode(): Int = delegate.hashCode()
  override def toString: String = ShowableTile.show(delegate)
}

object ShowableTile {
  private val maxCells = rfConfig.getInt("showable-max-cells")
  def show(tile: Tile): String = {
    val ct = tile.cellType
    val dims = tile.dimensions

    val data =
      if (tile.cellType.isFloatingPoint)
        tile.toArrayDouble().map {
          case c if isNoData(c) => "--"
          case c => c.toString
        } else tile.toArray().map {
          case c if isNoData(c) => "--"
          case c => c.toString
        }

    val cells =
      if(tile.size <= maxCells) {
        data.mkString("[", ",", "]")
      } else {
        val front = data.take(maxCells / 2).mkString("[", ",", "")
        val back = data.takeRight(maxCells / 2).mkString("", ",", "]")
        front + ",...," + back
      }
      s"[${ct.name}, $dims, $cells]"
    }
}
