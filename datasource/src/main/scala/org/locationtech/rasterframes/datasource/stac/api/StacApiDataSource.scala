package org.locationtech.rasterframes.datasource.stac.api

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class StacApiDataSource extends TableProvider with DataSourceRegister {

  def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType =
    getTable(null, Array.empty[Transform], caseInsensitiveStringMap.asCaseSensitiveMap()).schema()

  def getTable(structType: StructType, transforms: Array[Transform], map: util.Map[String, String]): Table =
    new StacApiTable()

  override def shortName(): String = "stac-api"
}

object StacApiDataSource {
  final val SHORT_NAME = "stac-api"
  final val URI_PARAM = "uri"
  final val SEARCH_FILTERS_PARAM = "search-filters"
  final val ASSET_LIMIT_PARAM = "asset-limit"
}
