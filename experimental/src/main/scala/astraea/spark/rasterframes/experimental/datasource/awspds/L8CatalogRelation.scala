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

import geotrellis.util.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.hadoop.fs.{Path ⇒ HadoopPath}
/**
 * Schema definition and parser for AWS PDS L8 scene data.
 *
 * @author sfitch
 * @since 9/28/17
 */
case class L8CatalogRelation(sqlContext: SQLContext, sceneListPath: HadoopPath)
  extends BaseRelation with TableScan with LazyLogging {

  def schema = StructType(Seq(
    StructField("productId", StringType, false),
    StructField("entityId", StringType, false),
    StructField("acquisitionDate", TimestampType, false),
    StructField("cloudCover", FloatType, false),
    StructField("processingLevel", StringType, false),
    StructField("path", ShortType, false),
    StructField("row", ShortType, false),
    StructField("min_lat", DoubleType, false),
    StructField("min_lon", DoubleType, false),
    StructField("max_lat", DoubleType, false),
    StructField("max_lon", DoubleType, false),
    StructField("download_url", StringType, false)
  ))

  def buildScan(): RDD[Row] = {
    import sqlContext.implicits._
    val catalog = sqlContext.read
      .schema(schema)
      .option("header", "true")
      .csv(sceneListPath.toString)
      .withColumn("url",  regexp_replace($"download_url", "index.html", ""))
      .drop("download_url")
      .withColumnRenamed("url", "download_url")
    catalog.rdd
  }
}


