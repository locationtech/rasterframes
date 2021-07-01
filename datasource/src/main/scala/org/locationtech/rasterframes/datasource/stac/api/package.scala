package org.locationtech.rasterframes.datasource.stac

import com.azavea.stac4s.api.client.SearchFilters
import eu.timepit.refined.types.numeric.NonNegInt
import org.apache.spark.sql.DataFrameReader
import io.circe.syntax._
import fs2.Stream

package object api {
  implicit class Fs2StreamOps[F[_], T](val self: Stream[F, T]) {
    def take(n: Option[Int]): Stream[F, T] = n.fold(self)(self.take(_))
  }

  implicit class DataFrameReaderOps(val self: DataFrameReader) extends AnyVal {
    def option(key: String, value: Option[String]): DataFrameReader = value.fold(self)(self.option(key, _))
    def option(key: String, value: Option[Int])(implicit d: DummyImplicit): DataFrameReader = value.fold(self)(self.option(key, _))
  }

  implicit class DataFrameReaderStacApiOps(val reader: DataFrameReader) extends AnyVal {
    def stacApi(): DataFrameReader = reader.format(StacApiDataSource.SHORT_NAME)
    def stacApi(uri: String, filters: SearchFilters = SearchFilters(), searchLimit: Option[NonNegInt] = None): DataFrameReader =
      stacApi()
        .option(StacApiDataSource.URI_PARAM, uri)
        .option(StacApiDataSource.SEARCH_FILTERS_PARAM, filters.asJson.noSpaces)
        .option(StacApiDataSource.ASSET_LIMIT_PARAM, searchLimit.map(_.value))
  }
}
