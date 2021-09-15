package org.locationtech.rasterframes.datasource.stac.api

import org.locationtech.rasterframes.datasource.stac.api.encoders._
import com.azavea.stac4s.api.client.SearchFilters
import eu.timepit.refined.types.numeric.NonNegInt
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.locationtech.rasterframes.datasource.stac.api.StacApiDataSource.{ASSET_LIMIT_PARAM, SEARCH_FILTERS_PARAM, URI_PARAM}
import org.locationtech.rasterframes.datasource.{intParam, jsonParam, uriParam}
import sttp.model.Uri

import scala.collection.JavaConverters._
import java.util

class StacApiTable extends Table with SupportsRead {
  import StacApiTable._

  def name(): String = this.getClass.toString

  def schema(): StructType = stacItemEncoder.schema

  def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

  def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new StacApiScanBuilder(options.uri, options.searchFilters, options.searchLimit)
}

object StacApiTable {
  implicit class CaseInsensitiveStringMapOps(val options: CaseInsensitiveStringMap) extends AnyVal {
    def uri: Uri = uriParam(URI_PARAM, options).getOrElse(throw new IllegalArgumentException("Missing STAC API URI."))

    def searchFilters: SearchFilters =
      jsonParam(SEARCH_FILTERS_PARAM, options)
        .flatMap(_.as[SearchFilters].toOption)
        .getOrElse(SearchFilters(limit = NonNegInt.from(30).toOption))

    def searchLimit: Option[NonNegInt] = intParam(ASSET_LIMIT_PARAM, options).flatMap(NonNegInt.from(_).toOption)
  }
}
