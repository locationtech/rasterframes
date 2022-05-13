package org.locationtech.rasterframes.datasource.shapefile

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class ShapeFileDataSource extends TableProvider with DataSourceRegister {

  def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType =
    getTable(null, Array.empty[Transform], caseInsensitiveStringMap.asCaseSensitiveMap()).schema()

  def getTable(structType: StructType, transforms: Array[Transform], map: util.Map[String, String]): Table =
    new ShapeFileTable()

  def shortName(): String = ShapeFileDataSource.SHORT_NAME
}

object ShapeFileDataSource {
  final val SHORT_NAME = "shapefile"
  final val URL_PARAM = "url"
}
