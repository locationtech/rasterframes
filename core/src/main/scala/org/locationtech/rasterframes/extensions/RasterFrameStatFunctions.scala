package org.locationtech.rasterframes.extensions

import org.locationtech.rasterframes.stats._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

final class RasterFrameStatFunctions private[rasterframes](df: DataFrame) {

    /**
      * Calculates the approximate quantiles of a numerical column of a DataFrame.
      *
      * The result of this algorithm has the following deterministic bound:
      * If the DataFrame has N elements and if we request the quantile at probability `p` up to error
      * `err`, then the algorithm will return a sample `x` from the DataFrame so that the *exact* rank
      * of `x` is close to (p * N).
      * More precisely,
      *
      * {{{
      *   floor((p - err) * N) <= rank(x) <= ceil((p + err) * N)
      * }}}
      *
      * This method implements a variation of the Greenwald-Khanna algorithm (with some speed
      * optimizations).
      * The algorithm was first present in <a href="http://dx.doi.org/10.1145/375663.375670">
      * Space-efficient Online Computation of Quantile Summaries</a> by Greenwald and Khanna.
      *
      * @param col the name of the numerical column
      * @param probabilities a list of quantile probabilities
      *   Each number must belong to [0, 1].
      *   For example 0 is the minimum, 0.5 is the median, 1 is the maximum.
      * @param relativeError The relative target precision to achieve (greater than or equal to 0).
      *   If set to zero, the exact quantiles are computed, which could be very expensive.
      *   Note that values greater than 1 are accepted but give the same result as 1.
      * @return the approximate quantiles at the given probabilities
      *
      * @note null and NaN values will be removed from the numerical column before calculation. If
      *   the dataframe is empty or the column only contains null or NaN, an empty array is returned.
      *
      * @since 2.0.0
      */
    def approxTileQuantile(
                        col: String,
                        probabilities: Array[Double],
                        relativeError: Double): Array[Double] = {
      approxTileQuantile(Array(col), probabilities, relativeError).head
    }

  /**
    * Calculates the approximate quantiles of numerical columns of a DataFrame.
    * @see `approxQuantile(col:Str* approxQuantile)` for detailed description.
    *
    * @param cols the names of the numerical columns
    * @param probabilities a list of quantile probabilities
    *   Each number must belong to [0, 1].
    *   For example 0 is the minimum, 0.5 is the median, 1 is the maximum.
    * @param relativeError The relative target precision to achieve (greater than or equal to 0).
    *   If set to zero, the exact quantiles are computed, which could be very expensive.
    *   Note that values greater than 1 are accepted but give the same result as 1.
    * @return the approximate quantiles at the given probabilities of each column
    *
    * @note null and NaN values will be ignored in numerical columns before calculation. For
    *   columns only containing null or NaN values, an empty array is returned.
    *
    */
  def approxTileQuantile(
                      cols: Array[String],
                      probabilities: Array[Double],
                      relativeError: Double): Array[Array[Double]] = {
    multipleApproxQuantiles(
      df.select(cols.map(col): _*),
      cols,
      probabilities,
      relativeError).map(_.toArray).toArray
  }

}
