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

package org.locationtech.rasterframes.util

import org.locationtech.rasterframes.ref.RasterRef.RasterRefTile
import org.locationtech.rasterframes.ref.RasterRef
import org.locationtech.rasterframes.ref.RasterSource._
import com.esotericsoftware.kryo.Kryo
import org.locationtech.rasterframes.ref.{RasterRef, RasterSource}


/**
 *
 * Kryo registrator.
 *
 * @since 10/29/18
 */
class RFKryoRegistrator extends geotrellis.spark.io.kryo.KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    super.registerClasses(kryo)
    kryo.register(classOf[RasterSource])
    kryo.register(classOf[RasterRef])
    kryo.register(classOf[RasterRefTile])
    kryo.register(classOf[DelegatingRasterSource])
    kryo.register(classOf[InMemoryRasterSource])
    kryo.register(classOf[HadoopGeoTiffRasterSource])
    kryo.register(classOf[GDALRasterSource])
    kryo.register(classOf[SimpleGeoTiffInfo])
    kryo.register(classOf[geotrellis.raster.io.geotiff.reader.GeoTiffReader.GeoTiffInfo])
  }
}
