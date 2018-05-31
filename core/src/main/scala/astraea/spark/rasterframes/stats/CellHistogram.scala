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
import scala.collection.mutable.{ListBuffer => MutableListBuffer}

/**
 * Container for computed aggregate histogram.
 *
 * @since 4/3/18
 */
case class CellHistogram(stats: CellStatistics, bins: Seq[CellHistogram.Bin]) {

  val labels = bins.map(_.value)
  def mean = stats.mean
  def totalCount = stats.dataCells
  def asciiStats = stats.asciiStats
  def asciiHistogram(width: Int = 80)= {
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

  // find the count of the bin that the value fits into
  def itemCount(label: Double): Long = {
    // look at each pair of consecutive bins, and when one bin is <= the value
    // and the other is > the value, return the smaller bin label
    val sorted = bins.sortBy(_.value)
    require(sorted.nonEmpty, "Sorted must be nonempty")
    require(label >= labels.min, "Label must be within the range of the values")
    val tBin = (0 until sorted.length - 1).find(i => sorted.apply(i).value <= label
      && label < sorted.apply(i + 1).value).getOrElse(sorted.length - 1)
    if (tBin != -1) {
      bins.seq.apply(tBin).count }
    else {
      Long.MaxValue
    }
  }

  private def cdfIntervals(): Iterator[((Double, Double), (Double, Double))] = {
    if(bins.size < 2) {
      Iterator.empty
    } else {
      val bs = bins
      val n = totalCount
      // We have to prepend the minimum value here
      val ds = bins.map(_.value).min +: bs.map(_.value)
      val pdf = bs.map(_.count.toDouble / n)
      val cdf = pdf.scanLeft(0.0)(_ + _)
      val data = ds.zip(cdf).sliding(2)

      data.map({ ab => (ab.head, ab.tail.head) })
    }
  }

  def percentileBreaks(qs: Seq[Double]): Seq[Double] = {
    if(bins.size == 1) {
      qs.map(z => bins.head.value)
    } else {
      val data = cdfIntervals
      if(!data.hasNext) {
        Seq()
      } else {
        val result = MutableListBuffer[Double]()
        var curr = data.next

        def getValue(q: Double): Double = {
          val (d1, pct1) = curr._1
          val (d2, pct2) = curr._2
          val proportionalDiff = (q - pct1) / (pct2 - pct1)
          (1 - proportionalDiff) * d1 + proportionalDiff * d2
        }

        val quantilesToCheck =
          if (qs.head < curr._2._2) {
            // The first case. Either the first bin IS the minimum
            // value or it is VERY close (because it is the result of
            // combining the minValue bin with neighboring bins)
            result += curr._1._1

            // IF the minvalue is the same as the lowest bin, we need
            // to clean house and remove the lowest bin.  Else, we
            // have to treat the lowest bin as the 0th pctile for
            // interpolation.
            if (curr._1._1 == curr._2._1) { curr = (curr._1, data.next._2) }
            else { curr = ((curr._1._1, 0.0), curr._2) }
            qs.tail
          } else {
            qs
          }

        for(q <- quantilesToCheck) {
          // Catch the edge case of 0th pctile, which usually won't matter
          if (q == 0.0) { result += bins.map(_.value).min}
          else if (q == 1.0) { result += bins.map(_.value).max}
          else {
            if(q < curr._2._2) {
              result += getValue(q)
            } else {
              while(data.hasNext && curr._2._2 <= q) { curr = data.next }
              result += getValue(q)
            }
          }
        }

        result
      }
    }
  }

  def quantileBreaks(breaks: Int): Array[Double] = {
    require(breaks > 0, "Breaks must be greater than 0")
    percentileBreaks((1 until breaks + 1).map(_ / (breaks + 1).toDouble)).toArray
  }
}

object CellHistogram {
  case class Bin(value: Double, count: Long)
  def apply(hist: GTHistogram[Int]): CellHistogram = {
    val stats = CellStatistics(hist.statistics().get)
    CellHistogram(stats, hist.binCounts().map(p ⇒ Bin(p._1.toDouble, p._2)))
  }
  def apply(hist: GTHistogram[Double])(implicit ev: DummyImplicit): CellHistogram = {
    val stats = hist.statistics().map(CellStatistics.apply).getOrElse(CellStatistics.empty)
    // Code should be this, but can't due to geotrellis#2664:
    // val bins = hist.binCounts().map(p ⇒ Bin(p._1, p._2))
    val bins = hist.asInstanceOf[StreamingHistogram].buckets().map(b ⇒ Bin(b.label, b.count))
    CellHistogram(stats, bins)
  }
}
