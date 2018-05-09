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
import astraea.spark.rasterframes.stats.CellHistogram.Bin
import geotrellis.raster.histogram.{Histogram ⇒ GTHistogram}

/**
 * Container for histogram computed
 *
 * @since 4/3/18
 */
case class CellHistogram(stats: CellStatistics, bins: Seq[Bin]) {
  def mean = stats.mean
  def totalCount = stats.dataCells
}

object CellHistogram {
  case class Bin(value: Double, count: Long)
  def apply(hist: GTHistogram[Int]): CellHistogram = {
    val stats = CellStatistics(hist.statistics().get)
    CellHistogram(stats, hist.binCounts().map(p ⇒ Bin(p._1.toDouble, p._2)))
  }
  def apply(hist: GTHistogram[Double])(implicit ev: DummyImplicit): CellHistogram = {
    val stats = CellStatistics(hist.statistics().get)
    CellHistogram(stats, hist.binCounts().map(p ⇒ Bin(p._1, p._2)))
  }
}
