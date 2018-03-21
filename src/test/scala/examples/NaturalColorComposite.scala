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

package examples


import java.nio.file.{CopyOption, StandardCopyOption}

import geotrellis.raster.{MultibandTile, UByteConstantNoDataCellType}
import geotrellis.raster.io.geotiff.{GeoTiff, SinglebandGeoTiff}
import geotrellis.raster.render._

/**
 *
 * @author sfitch 
 * @since 9/25/17
 */
object NaturalColorComposite extends App {
  def readTiff(name: String): SinglebandGeoTiff =
    SinglebandGeoTiff(getClass.getResource(s"/$name").getPath)

  val filenamePattern = "L8-B%d-Elkton-VA.tiff"

  val tiles = Seq(4, 3, 2)
    .map(i ⇒ filenamePattern.format(i))
    .map(readTiff)
    .map(_.tile)
    .map { tile ⇒
      val (min, max) = tile.findMinMax
      val normalized = tile.normalize(min, max, 1, 1 << 9)
      normalized.convert(UByteConstantNoDataCellType)
    }

  MultibandTile(tiles)
    .renderPng()
    .write("rgb.png")

}
