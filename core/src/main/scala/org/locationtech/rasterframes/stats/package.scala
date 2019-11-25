package org.locationtech.rasterframes

import geotrellis.raster.Tile
import org.apache.spark.sql.catalyst.util.QuantileSummaries
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.locationtech.rasterframes.expressions.DynamicExtractors._
import org.locationtech.rasterframes.expressions.accessors.ExtractTile


package object stats {

  def multipleApproxQuantiles(df: DataFrame,
                              cols: Seq[String],
                              probabilities: Seq[Double],
                              relativeError: Double): Seq[Seq[Double]] = {
    require(relativeError >= 0,
      s"Relative Error must be non-negative but got $relativeError")

    val columns: Seq[Column] = cols.map { colName =>
      val field = df.schema(colName)

      require(tileExtractor.isDefinedAt(field.dataType),
        s"Quantile calculation for column $colName with data type ${field.dataType}" +
          " is not supported; it must be Tile-like.")
      ExtractTile(new Column(colName))
    }

    val emptySummaries = Array.fill(cols.size)(
      new QuantileSummaries(QuantileSummaries.defaultCompressThreshold, relativeError))

    def apply(summaries: Array[QuantileSummaries], row: Row): Array[QuantileSummaries] = {
      var i = 0
      while (i < summaries.length) {
        if (!row.isNullAt(i)) {
          val t: Tile = row.getAs[Tile](i)
          // now insert all the tile values into the summary for this column
          t.foreachDouble(v ⇒
            if (!v.isNaN) summaries(i) = summaries(i).insert(v)
          )
        }
        i += 1  // next column
      }
      summaries
    }

    def merge(
               sum1: Array[QuantileSummaries],
               sum2: Array[QuantileSummaries]): Array[QuantileSummaries] = {
      sum1.zip(sum2).map { case (s1, s2) => s1.compress().merge(s2.compress()) }
    }
    val summaries = df.select(columns: _*).rdd.treeAggregate(emptySummaries)(apply, merge)

    summaries.map { summary => probabilities.flatMap(summary.query) }
  }

}
