/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea, Inc.
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
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.locationtech.rasterframes.experimental.datasource.awspds

import better.files.File
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.locationtech.rasterframes.experimental.datasource.CachedDatasetRelation

/**
 * Constructs a dataframe from the available scenes
 *
 * @since 5/4/18
 */
case class MODISCatalogRelation(sqlContext: SQLContext, sceneList: File)
  extends BaseRelation with TableScan with CachedDatasetRelation {
  import MODISCatalogRelation._

  protected def cacheFile: File = sceneList.parent / (sceneList.nameWithoutExtension + ".parquet")

  override def schema: StructType = MODISCatalogRelation.schema

  protected def constructDataset: Dataset[Row] = {
    import sqlContext.implicits._

    logger.info("Parsing " + sceneList)
    val catalog = sqlContext.read
      .option("header", "false")
      .option("mode", "DROPMALFORMED") // <--- mainly for the fact that we have internal headers from the concat
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .schema(MODISCatalogRelation.inputSchema)
      .csv(sceneList.toString)

    val bandCols = Bands.values.toSeq.map(b => MCD43A4_band_url(b) as b.toString)

    catalog
      .withColumn("__split_gid", split($"gid", "\\."))
      .withColumn(DOWNLOAD_URL.name, regexp_replace(col(DOWNLOAD_URL.name), "index.html", ""))
      .select(Seq(
        $"__split_gid" (0) as PRODUCT_ID.name,
        $"date" as ACQUISITION_DATE.name,
        $"__split_gid" (2) as GRANULE_ID.name,
        $"${GID.name}") ++ bandCols: _*
      )
      .orderBy(ACQUISITION_DATE.name, GID.name)
      .repartition(defaultNumPartitions, col(GRANULE_ID.name))
  }
}

object MODISCatalogRelation extends PDSFields {

  def MCD43A4_band_url(suffix: Bands.Band) =
    concat(col(DOWNLOAD_URL.name), concat(col(GID.name), lit(s"_${suffix}.TIF")))

  object Bands extends Enumeration {
    type Band = Value
    val B01, B01qa, B02, B02qa, B03, B03aq, B04, B04qa, B05, B05qa, B06, B06qa, B07, B07qa = Value
    val names: Seq[String] = values.toSeq.map(_.toString)
  }

  def schema = StructType(Seq(
    PRODUCT_ID,
    ACQUISITION_DATE,
    GRANULE_ID,
    GID
  ) ++ Bands.names.map(n => StructField(n, StringType, true)))

  private val inputSchema = StructType(Seq(
    StructField("date", TimestampType, false),
    DOWNLOAD_URL,
    GID
  ))
}
