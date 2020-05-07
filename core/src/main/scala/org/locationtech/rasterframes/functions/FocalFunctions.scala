/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2020 Astraea, Inc.
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

package org.locationtech.rasterframes.functions

import geotrellis.raster.Neighborhood
import geotrellis.raster.mapalgebra.focal.Kernel
import org.apache.spark.sql.Column
import org.locationtech.rasterframes.expressions.focalops._

trait FocalFunctions {
  def rf_focal_mean(tileCol: Column, neighborhood: Neighborhood): Column =
    FocalMean(tileCol, neighborhood)

  def rf_focal_median(tileCol: Column, neighborhood: Neighborhood): Column =
    FocalMedian(tileCol, neighborhood)

  def rf_focal_mode(tileCol: Column, neighborhood: Neighborhood): Column =
    FocalMode(tileCol, neighborhood)

  def rf_focal_max(tileCol: Column, neighborhood: Neighborhood): Column =
    FocalMax(tileCol, neighborhood)

  def rf_focal_min(tileCol: Column, neighborhood: Neighborhood): Column =
    FocalMin(tileCol, neighborhood)

  def rf_focal_stddev(tileCol: Column, neighborhood: Neighborhood): Column =
    FocalStdDev(tileCol, neighborhood)

  def rf_focal_moransi(tileCol: Column, neighborhood: Neighborhood): Column =
    FocalMoransI(tileCol, neighborhood)

  def rf_convolve(tileCol: Column, kernel: Kernel): Column =
    Convolve(tileCol, kernel)
}
