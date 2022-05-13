package org.locationtech.rasterframes.datasource.shapefile

import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.locationtech.rasterframes.datasource.shapefile.ShapeFileDataSource.URL_PARAM
import org.locationtech.rasterframes.datasource.urlParam
import java.net.URL

import scala.collection.JavaConverters._
import java.util

class ShapeFileTable extends Table with SupportsRead {
  import ShapeFileTable._

  def name(): String = this.getClass.toString

  def schema(): StructType = geometryExpressionEncoder.schema

  def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

  def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new ShapeFileScanBuilder(options.url)
}

object ShapeFileTable {
  implicit class CaseInsensitiveStringMapOps(val options: CaseInsensitiveStringMap) extends AnyVal {
    def url: URL = urlParam(URL_PARAM, options).getOrElse(throw new IllegalArgumentException("Missing URL."))
  }
}
