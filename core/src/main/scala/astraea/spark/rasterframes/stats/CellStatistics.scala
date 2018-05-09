/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea. Inc.
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
 *
 */

package astraea.spark.rasterframes.stats

/**
 * Container for computed statistics over cells.
 *
 * @since 4/3/18
 */
case class CellStatistics(dataCells: Long, min: Double, max: Double, mean: Double, variance: Double) {
  def stddev: Double = math.sqrt(variance)
  def asciiStats = Seq(
    "dataCells: " + dataCells,
    "min: " + min,
    "max: " + max,
    "mean: " + mean,
    "variance: " + variance,
    "stddev: " + math.sqrt(variance)
  ).mkString("\n")
}
object CellStatistics {
  // Convert GeoTrellis stats object into our simplified one.
  def apply(stats: geotrellis.raster.summary.Statistics[Double]) =
    new CellStatistics(stats.dataCells, stats.zmin, stats.zmax, stats.mean, stats.stddev * stats.stddev)

  def apply(stats: geotrellis.raster.summary.Statistics[Int])(implicit d: DummyImplicit) =
    new CellStatistics(stats.dataCells, stats.zmin.toDouble, stats.zmax.toDouble, stats.mean, stats.stddev * stats.stddev)
}