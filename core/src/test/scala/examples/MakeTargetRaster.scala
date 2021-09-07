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

import geotrellis.proj4.CRS
import geotrellis.raster._
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster.mapalgebra.local.TileReducer
import geotrellis.util.Filesystem
import geotrellis.vector._


/**
 * Utility for creating a target
 *
 * @since 9/21/17
 */
object MakeTargetRaster extends App {
  object Flattener extends TileReducer(
    (l: Int, r: Int) => if (isNoData(r)) l else r
  )(
    (l: Double, r: Double) => if (isNoData(r)) l else r
  )

  val tiff = SinglebandGeoTiff(getClass.getResource("/L8-B2-Elkton-VA.tiff").getPath)
  val json = Filesystem.readText(getClass.getResource("/L8-Labels-Elkton-VA.geojson").getPath)
  val wgs84 = CRS.fromEpsgCode(4326)

  val features = json.extractFeatures[Feature[Polygon, Map[String, Int]]]()

  val layers = for {
    f ← features
    pf = f.reproject(wgs84, tiff.crs)
    raster = pf.geom.rasterizeWithValue(tiff.rasterExtent, f.data("id"), UByteUserDefinedNoDataCellType(255.toByte))
  } yield raster

  val result = Flattener(layers.map(_.tile))

  SinglebandGeoTiff(result, tiff.extent, tiff.crs).write("L8-Labels-Elkton-VA.tiff")

}
