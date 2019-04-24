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

package org.locationtech.rasterframes.stats

import geotrellis.raster.Tile
import org.apache.spark.sql.types.StructType
import org.locationtech.rasterframes.encoders.StandardEncoders

/**
 * Container for computed statistics over cells.
 *
 * @since 4/3/18
 */
case class CellStatistics(data_cells: Long, no_data_cells: Long, min: Double, max: Double, mean: Double, variance: Double) {
  def stddev: Double = math.sqrt(variance)
  def asciiStats = Seq(
    "data_cells: " + data_cells,
    "no_data_cells: " + no_data_cells,
    "min: " + min,
    "max: " + max,
    "mean: " + mean,
    "variance: " + variance,
    "stddev: " + math.sqrt(variance)
  ).mkString("\n")

  override def toString: String = {
    val fields = Seq("data_cells", "no_data_cells", "min", "max", "mean", "variance")
    fields.iterator
      .zip(productIterator)
      .map(p ⇒ p._1 + "=" + p._2)
      .mkString(productPrefix + "(", ",", ")")
  }
}
object CellStatistics {
  // Convert GeoTrellis stats object into our simplified one.
  private[stats]
  def apply(stats: geotrellis.raster.summary.Statistics[Double]) =
    new CellStatistics(stats.dataCells, -1, stats.zmin, stats.zmax, stats.mean, stats.stddev * stats.stddev)

  private[stats]
  def apply(stats: geotrellis.raster.summary.Statistics[Int])(implicit d: DummyImplicit) =
    new CellStatistics(stats.dataCells, -1, stats.zmin.toDouble, stats.zmax.toDouble, stats.mean, stats.stddev * stats.stddev)

  def apply(tile: Tile): Option[CellStatistics] = {
    val base = if (tile.cellType.isFloatingPoint) tile.statisticsDouble.map(CellStatistics.apply)
    else tile.statistics.map(CellStatistics.apply)
    base.map(s => s.copy(no_data_cells = tile.size - s.data_cells))
  }

  def empty = new CellStatistics(0, 0, Double.NaN, Double.NaN, Double.NaN, Double.NaN)

  lazy val schema: StructType = StandardEncoders.cellStatsEncoder.schema
}