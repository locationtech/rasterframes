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
import org.apache.spark.sql.functions.lit
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.encoders.serialized_literal
import org.locationtech.rasterframes.expressions.focalops._

trait FocalFunctions {
  def rf_focal_mean(tileCol: Column, neighborhood: Neighborhood): Column =
    rf_focal_mean(tileCol, serialized_literal(neighborhood))

  def rf_focal_mean(tileCol: Column, neighborhoodCol: Column): Column =
    FocalMean(tileCol, neighborhoodCol)

  def rf_focal_median(tileCol: Column, neighborhood: Neighborhood): Column =
    rf_focal_median(tileCol, serialized_literal(neighborhood))

  def rf_focal_median(tileCol: Column, neighborhoodCol: Column): Column =
    FocalMedian(tileCol, neighborhoodCol)

  def rf_focal_mode(tileCol: Column, neighborhood: Neighborhood): Column =
    rf_focal_mode(tileCol, serialized_literal(neighborhood))

  def rf_focal_mode(tileCol: Column, neighborhoodCol: Column): Column =
    FocalMode(tileCol, neighborhoodCol)

  def rf_focal_max(tileCol: Column, neighborhood: Neighborhood): Column =
    rf_focal_max(tileCol, serialized_literal(neighborhood))

  def rf_focal_max(tileCol: Column, neighborhoodCol: Column): Column =
    FocalMax(tileCol, neighborhoodCol)

  def rf_focal_min(tileCol: Column, neighborhood: Neighborhood): Column =
    rf_focal_min(tileCol, serialized_literal(neighborhood))

  def rf_focal_min(tileCol: Column, neighborhoodCol: Column): Column =
    FocalMin(tileCol, neighborhoodCol)

  def rf_focal_stddev(tileCol: Column, neighborhood: Neighborhood): Column =
    rf_focal_stddev(tileCol, serialized_literal(neighborhood))

  def rf_focal_stddev(tileCol: Column, neighborhoodCol: Column): Column =
    FocalStdDev(tileCol, neighborhoodCol)

  def rf_focal_moransi(tileCol: Column, neighborhood: Neighborhood): Column =
    rf_focal_moransi(tileCol, serialized_literal(neighborhood))

  def rf_focal_moransi(tileCol: Column, neighborhoodCol: Column): Column =
    FocalMoransI(tileCol, neighborhoodCol)

  def rf_convolve(tileCol: Column, kernel: Kernel): Column =
    rf_convolve(tileCol, serialized_literal(kernel))

  def rf_convolve(tileCol: Column, kernelCol: Column): Column =
    Convolve(tileCol, kernelCol)

  def rf_slope[T: Numeric](tileCol: Column, zFactor: T): Column =
    rf_slope(tileCol, lit(zFactor))

  def rf_slope(tileCol: Column, zFactorCol: Column): Column =
    Slope(tileCol, zFactorCol)

  def rf_aspect(tileCol: Column): Column =
    Aspect(tileCol)

  def rf_hillshade[T: Numeric](tileCol: Column, azimuth: T, altitude: T, zFactor: T): Column =
    rf_hillshade(tileCol, lit(azimuth), lit(altitude), lit(zFactor))

  def rf_hillshade(tileCol: Column, azimuth: Column, altitude: Column, zFactor: Column): Column =
    Hillshade(tileCol, azimuth, altitude, zFactor)
}
