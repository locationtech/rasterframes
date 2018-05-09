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
import geotrellis.raster.histogram.{StreamingHistogram, Histogram ⇒ GTHistogram}

/**
 * Container for computed aggregate histogram.
 *
 * @since 4/3/18
 */
case class CellHistogram(stats: CellStatistics, bins: Seq[CellHistogram.Bin]) {
  def mean = stats.mean
  def totalCount = stats.dataCells
  def asciiStats = stats.asciiStats
  def asciiHistogram(width: Int = 80)= {
    val labels = bins.map(_.value)
    val counts = bins.map(_.count)
    val maxCount = counts.max.toFloat
    val maxLabelLen = labels.map(_.toString.length).max
    val maxCountLen = counts.map(c ⇒ f"$c%,d".length).max
    val fmt = s"%${maxLabelLen}s: %,${maxCountLen}d | %s"
    val barlen = width - fmt.format(0, 0, "").length

    val lines = for {
      (l, c) ← labels.zip(counts)
    } yield {
      val width = (barlen * (c/maxCount)).round
      val bar = "*" * width
      fmt.format(l, c, bar)
    }

    lines.mkString("\n")
  }
}

object CellHistogram {
  case class Bin(value: Double, count: Long)
  def apply(hist: GTHistogram[Int]): CellHistogram = {
    val stats = CellStatistics(hist.statistics().get)
    CellHistogram(stats, hist.binCounts().map(p ⇒ Bin(p._1.toDouble, p._2)))
  }
  def apply(hist: GTHistogram[Double])(implicit ev: DummyImplicit): CellHistogram = {
    val stats = CellStatistics(hist.statistics().get)
    // Code should be this, but can't due to geotrellis#2664:
    // val bins = hist.binCounts().map(p ⇒ Bin(p._1, p._2))
    val bins = hist.asInstanceOf[StreamingHistogram].buckets().map(b ⇒ Bin(b.label, b.count))
    CellHistogram(stats, bins)
  }
}
