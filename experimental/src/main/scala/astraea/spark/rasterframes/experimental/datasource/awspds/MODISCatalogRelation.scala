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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/**
 * Constructs a dataframe from the available scenes
 *
 * @since 5/4/18
 */
case class MODISCatalogRelation(sqlContext: SQLContext, sceneListPath: String)
  extends BaseRelation with TableScan {

  private val inputSchema = StructType(Seq(
    StructField("date", DateType, false),
    StructField("download_url", StringType, false),
    StructField("gid", StringType, false)
  ))

  private lazy val preloaded = {
    import sqlContext.implicits._
    val catalog = sqlContext.read
      .option("header", "true")
      .option("mode", "FAILFAST")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .schema(inputSchema)
      .csv(sceneListPath)

    val result = catalog
      .withColumn("split_gid", split($"gid", "\\."))
      .select(
        $"split_gid"(0) as "productId",
        $"date" as "acquisitionDate",
        $"split_gid"(2) as "granuleId",
        regexp_replace($"download_url", "index.html", "") as "download_url",
        $"gid"
      )
      .drop($"split_gid")
    result
  }

  def schema = preloaded.schema

  def buildScan(): RDD[Row] = preloaded.rdd
}


