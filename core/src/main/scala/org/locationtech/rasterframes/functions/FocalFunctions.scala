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

import geotrellis.raster.{Neighborhood, TargetCell}
import geotrellis.raster.mapalgebra.focal.Kernel
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.encoders.serialized_literal
import org.locationtech.rasterframes.expressions.focalops._

trait FocalFunctions {
  def rf_focal_mean(tileCol: Column, neighborhood: Neighborhood): Column =
    rf_focal_mean(tileCol, neighborhood, TargetCell.All)

  def rf_focal_mean(tileCol: Column, neighborhood: Neighborhood, target: TargetCell): Column =
    rf_focal_mean(tileCol, serialized_literal(neighborhood), serialized_literal(target))

  def rf_focal_mean(tileCol: Column, neighborhoodCol: Column, targetCol: Column): Column =
    FocalMean(tileCol, neighborhoodCol, targetCol)

  def rf_focal_median(tileCol: Column, neighborhood: Neighborhood): Column =
    rf_focal_median(tileCol, neighborhood, TargetCell.All)

  def rf_focal_median(tileCol: Column, neighborhood: Neighborhood, target: TargetCell): Column =
    rf_focal_median(tileCol, serialized_literal(neighborhood), serialized_literal(target))

  def rf_focal_median(tileCol: Column, neighborhoodCol: Column, targetCol: Column): Column =
    FocalMedian(tileCol, neighborhoodCol, targetCol)

  def rf_focal_mode(tileCol: Column, neighborhood: Neighborhood): Column =
    rf_focal_mode(tileCol, neighborhood, TargetCell.All)

  def rf_focal_mode(tileCol: Column, neighborhood: Neighborhood, target: TargetCell): Column =
    rf_focal_mode(tileCol, serialized_literal(neighborhood), serialized_literal(target))

  def rf_focal_mode(tileCol: Column, neighborhoodCol: Column, targetCol: Column): Column =
    FocalMode(tileCol, neighborhoodCol, targetCol)

  def rf_focal_max(tileCol: Column, neighborhood: Neighborhood): Column =
    rf_focal_max(tileCol, neighborhood, TargetCell.All)

  def rf_focal_max(tileCol: Column, neighborhood: Neighborhood, target: TargetCell): Column =
    rf_focal_max(tileCol, serialized_literal(neighborhood), serialized_literal(target))

  def rf_focal_max(tileCol: Column, neighborhoodCol: Column, targetCol: Column): Column =
    FocalMax(tileCol, neighborhoodCol, targetCol)

  def rf_focal_min(tileCol: Column, neighborhood: Neighborhood): Column =
    rf_focal_min(tileCol, neighborhood, TargetCell.All)

  def rf_focal_min(tileCol: Column, neighborhood: Neighborhood, target: TargetCell): Column =
    rf_focal_min(tileCol, serialized_literal(neighborhood), serialized_literal(target))

  def rf_focal_min(tileCol: Column, neighborhoodCol: Column, targetCol: Column): Column =
    FocalMin(tileCol, neighborhoodCol, targetCol)

  def rf_focal_stddev(tileCol: Column, neighborhood: Neighborhood): Column =
    rf_focal_stddev(tileCol, neighborhood, TargetCell.All)

  def rf_focal_stddev(tileCol: Column, neighborhood: Neighborhood, target: TargetCell): Column =
    rf_focal_stddev(tileCol, serialized_literal(neighborhood), serialized_literal(target))

  def rf_focal_stddev(tileCol: Column, neighborhoodCol: Column, targetCol: Column): Column =
    FocalStdDev(tileCol, neighborhoodCol, targetCol)

  def rf_focal_moransi(tileCol: Column, neighborhood: Neighborhood): Column =
    rf_focal_moransi(tileCol, neighborhood, TargetCell.All)

  def rf_focal_moransi(tileCol: Column, neighborhood: Neighborhood, target: TargetCell): Column =
    rf_focal_moransi(tileCol, serialized_literal(neighborhood), serialized_literal(target))

  def rf_focal_moransi(tileCol: Column, neighborhoodCol: Column, targetCol: Column): Column =
    FocalMoransI(tileCol, neighborhoodCol, targetCol)

  def rf_convolve(tileCol: Column, kernel: Kernel): Column =
    rf_convolve(tileCol, kernel, TargetCell.All)

  def rf_convolve(tileCol: Column, kernel: Kernel, target: TargetCell): Column =
    rf_convolve(tileCol, serialized_literal(kernel), serialized_literal(target))

  def rf_convolve(tileCol: Column, kernelCol: Column, targetCol: Column): Column =
    Convolve(tileCol, kernelCol, targetCol)

  def rf_slope(tileCol: Column, zFactor: Double): Column =
    rf_slope(tileCol, zFactor, TargetCell.All)

  def rf_slope(tileCol: Column, zFactor: Double, target: TargetCell): Column =
    rf_slope(tileCol, lit(zFactor), serialized_literal(target))

  def rf_slope(tileCol: Column, zFactorCol: Column, targetCol: Column): Column =
    Slope(tileCol, zFactorCol, targetCol)

  def rf_aspect(tileCol: Column): Column =
    rf_aspect(tileCol, TargetCell.All)

  def rf_aspect(tileCol: Column, target: TargetCell): Column =
    rf_aspect(tileCol, serialized_literal(target))

  def rf_aspect(tileCol: Column, targetCol: Column): Column =
    Aspect(tileCol, targetCol)

  def rf_hillshade(tileCol: Column, azimuth: Double, altitude: Double, zFactor: Double): Column =
    rf_hillshade(tileCol, azimuth, altitude, zFactor, TargetCell.All)

  def rf_hillshade(tileCol: Column, azimuth: Double, altitude: Double, zFactor: Double, target: TargetCell): Column =
    rf_hillshade(tileCol, lit(azimuth), lit(altitude), lit(zFactor), serialized_literal(target))

  def rf_hillshade(tileCol: Column, azimuthCol: Column, altitudeCol: Column, zFactorCol: Column, targetCol: Column): Column =
    Hillshade(tileCol, azimuthCol, altitudeCol, zFactorCol, targetCol)
}
