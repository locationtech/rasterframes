/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Astraea, Inc.
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
 */

package org.locationtech.rasterframes

import java.net.URI

import geotrellis.raster.{ArrayTile, CellType, NODATA, Tile, isNoData}

/**
 * Module utils.
 *
 * @author sfitch 
 * @since 10/4/17
 */
package object bench {
  val rnd = new scala.util.Random(42)

  /** Construct a tile of given size and cell type populated with random values. */
  def randomTile(cols: Int, rows: Int, cellTypeName: String): Tile = {
    val cellType = CellType.fromName(cellTypeName)
    val tile = ArrayTile.alloc(cellType, cols, rows)
    if(cellType.isFloatingPoint) {
      tile.mapDouble(_ => rnd.nextGaussian())
    }
    else {
      tile.map(_ => {
        var c = NODATA
        do {
          c = rnd.nextInt(255)
        } while(isNoData(c))
        c
      })
    }
  }

  private val baseCOG = "https://s3-us-west-2.amazonaws.com/landsat-pds/c1/L8/149/039/LC08_L1TP_149039_20170411_20170415_01_T1/LC08_L1TP_149039_20170411_20170415_01_T1_%s.TIF"
  lazy val remoteCOGSingleband1 = URI.create(baseCOG.format("B1"))
  lazy val remoteCOGSingleband2 = URI.create(baseCOG.format("B2"))

}
