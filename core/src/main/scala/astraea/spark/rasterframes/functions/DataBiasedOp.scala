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

package astraea.spark.rasterframes.functions

import geotrellis.raster
import geotrellis.raster.isNoData
import geotrellis.raster.mapalgebra.local.LocalTileBinaryOp

/**
 * Variations of the GeoTrellis local algebra operations, except that
 * NoData combined with a value results in the value being returned.
 * @since 10/5/17
 */
object DataBiasedOp {
  object BiasedMin extends DataBiasedOp {
    def op(z1: Int, z2: Int) = math.min(z1, z2)
    def op(z1: Double, z2: Double) = math.min(z1, z2)
  }

  object BiasedMax extends DataBiasedOp {
    def op(z1: Int, z2: Int) = math.max(z1, z2)
    def op(z1: Double, z2: Double) = math.max(z1, z2)
  }

  object BiasedAdd extends DataBiasedOp {
    def op(z1: Int, z2: Int) = z1 + z2
    def op(z1: Double, z2: Double) = z1 + z2
  }
}
trait DataBiasedOp extends LocalTileBinaryOp {
  def op(z1: Int, z2: Int): Int
  def op(z1: Double, z2: Double): Double

  def combine(z1: Int, z2: Int): Int =
    if (isNoData(z1) && isNoData(z2)) raster.NODATA
    else if (isNoData(z1)) z2
    else if (isNoData(z2)) z1
    else op(z1, z2)

  def combine(z1: Double, z2: Double): Double =
    if (isNoData(z1) && isNoData(z2)) raster.doubleNODATA
    else if (isNoData(z1)) z2
    else if (isNoData(z2)) z1
    else op(z1, z2)
}
