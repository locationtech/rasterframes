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

package astraea.spark.rasterframes.experimental.datasource.awspds

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{Path ⇒ HadoopPath}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/**
 * Constructs a dataframe from the available scenes
 *
 * @since 5/4/18
 */
case class MODISCatalogRelation(sqlContext: SQLContext, sceneList: HadoopPath)
  extends BaseRelation with TableScan with CachedDatasetRelation with LazyLogging {

  import MODISCatalogRelation._

  protected def cacheFile: HadoopPath = sceneList.suffix(".parquet")

  private val inputSchema = StructType(Seq(
    StructField("date", TimestampType, false),
    DOWNLOAD_URL,
    GID
  ))

  def schema = StructType(Seq(
    PRODUCT_ID,
    ACQUISITION_DATE,
    GRANULE_ID,
    GID,
    ASSETS
  ))

  protected def constructDataset: Dataset[Row] = {
    import sqlContext.implicits._

    logger.debug("Parsing " + sceneList)
    val catalog = sqlContext.read
      .option("header", "true")
      .option("mode", "DROPMALFORMED") // <--- mainly for the fact that we have internal headers from the concat
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .schema(inputSchema)
      .csv(sceneList.toString)

    catalog
      .withColumn("__split_gid", split($"gid", "\\."))
      .withColumn(DOWNLOAD_URL.name, regexp_replace(col(DOWNLOAD_URL.name), "index.html", ""))
      .select(
        $"__split_gid" (0) as PRODUCT_ID.name,
        $"date" as ACQUISITION_DATE.name,
        $"__split_gid" (2) as GRANULE_ID.name,
        $"${GID.name}",
        MCD43A4_BAND_MAP as ASSETS.name
      )
      .orderBy(ACQUISITION_DATE.name, GID.name)
      .repartition(col(GRANULE_ID.name))
  }
}

object MODISCatalogRelation extends PDSFields {

  def MCD43A4_LINK(suffix: String) =
    concat(col(DOWNLOAD_URL.name), concat(col(GID.name), lit(suffix)))

  val MCD43A4_BAND_MAP = map(
    lit("B01"), MCD43A4_LINK("_B01.TIF"),
    lit("B01qa"), MCD43A4_LINK("_B01qa.TIF"),
    lit("B02"), MCD43A4_LINK("_B02.TIF"),
    lit("B02qa"), MCD43A4_LINK("_B02qa.TIF"),
    lit("B03"), MCD43A4_LINK("_B03.TIF"),
    lit("B03qa"), MCD43A4_LINK("_B03qa.TIF"),
    lit("B04"), MCD43A4_LINK("_B04.TIF"),
    lit("B04qa"), MCD43A4_LINK("_B04qa.TIF"),
    lit("B05"), MCD43A4_LINK("_B05.TIF"),
    lit("B05qa"), MCD43A4_LINK("_B05qa.TIF"),
    lit("B06"), MCD43A4_LINK("_B06.TIF"),
    lit("B06qa"), MCD43A4_LINK("_B06qa.TIF"),
    lit("B07"), MCD43A4_LINK("_B07.TIF"),
    lit("B07qa"), MCD43A4_LINK("_B07qa.TIF"),
    lit("metadata.json"), MCD43A4_LINK("_meta.json"),
    lit("metadata.xml"), MCD43A4_LINK(".hdf.xml"),
    lit("index.html"), concat(col(DOWNLOAD_URL.name), lit("index.html"))
  )

}
