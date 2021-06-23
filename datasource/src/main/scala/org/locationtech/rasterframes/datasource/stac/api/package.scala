package org.locationtech.rasterframes.datasource.stac

import com.azavea.stac4s.api.client.SearchFilters
import eu.timepit.refined.types.numeric.NonNegInt
import org.apache.spark.sql.DataFrameReader
import io.circe.syntax._

package object api {
  implicit class DataFrameReaderHasGeoJson(val reader: DataFrameReader) extends AnyVal {
    def stacApi(): DataFrameReader = reader.format(StacApiDataSource.SHORT_NAME)
    def stacApi(uri: String, filters: SearchFilters = SearchFilters(), searchLimit: Option[NonNegInt] = None): DataFrameReader = {
      val reader = stacApi()
        .option(StacApiDataSource.URI_PARAM, uri)
        .option(StacApiDataSource.SEARCH_FILTERS_PARAM, filters.asJson.noSpaces)

      searchLimit.fold(reader)(i => reader.option(StacApiDataSource.ASSET_LIMIT_PARAM, i.toString))
    }
  }
}
