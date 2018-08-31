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
  extends BaseRelation with TableScan with PDSFields with CachedDatasetRelation with LazyLogging {

  protected def cacheFile: HadoopPath = sceneList.suffix(".parquet")

  private val inputSchema = StructType(Seq(
    StructField("date", DateType, false),
    DOWNLOAD_URL,
    GID
  ))

  def schema = StructType(Seq(
    PRODUCT_ID,
    ACQUISITION_DATE,
    GRANULE_ID,
    DOWNLOAD_URL,
    GID
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
      .select(
        $"__split_gid"(0) as PRODUCT_ID.name,
        $"date" as ACQUISITION_DATE.name,
        $"__split_gid"(2) as GRANULE_ID.name,
        regexp_replace($"download_url", "index.html", "") as DOWNLOAD_URL.name,
        $"${GID.name}"
      )
      .drop($"__split_gid")
      .orderBy(ACQUISITION_DATE.name, GID.name)
      .repartition(8)

  }
}
