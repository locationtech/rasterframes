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

import com.vividsolutions.jts.geom.Envelope
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
  extends BaseRelation with TableScan with PDSFields with LazyLogging {

  def inputSchema = StructType(Seq(
    PRODUCT_ID,
    ENTITY_ID,
    ACQUISITION_DATE,
    CLOUD_COVER,
    PROC_LEVEL,
    PATH,
    ROW,
    StructField("min_lat", DoubleType, false),
    StructField("min_lon", DoubleType, false),
    StructField("max_lat", DoubleType, false),
    StructField("max_lon", DoubleType, false),
    DOWNLOAD_URL
  ))

  def schema = StructType(Seq(
    PRODUCT_ID,
    ENTITY_ID,
    ACQUISITION_DATE,
    CLOUD_COVER,
    PROC_LEVEL,
    PATH,
    ROW,
    BOUNDS_WGS84,
    DOWNLOAD_URL
  ))

  def buildScan(): RDD[Row] = {
    import sqlContext.implicits._
    import astraea.spark.rasterframes.encoders.StandardEncoders.envelopeEncoder
    val catalog = sqlContext.read
      .schema(inputSchema)
      .option("header", "true")
      .csv(sceneListPath.toString)
      .withColumn("__url",  regexp_replace($"download_url", "index.html", ""))
      .where(not($"${PRODUCT_ID.name}".endsWith("RT")))
      .drop("download_url")
      .withColumn(BOUNDS_WGS84.name, struct(
        $"min_lon" as "minX",
        $"max_lon" as "maxX",
        $"min_lat" as "minY",
        $"max_lat" as "maxY"
      ).as[Envelope])
      .withColumnRenamed("__url", DOWNLOAD_URL.name)
      .select(schema.map(f ⇒ col(f.name)): _*)
      .orderBy(ACQUISITION_DATE.name, PATH.name, ROW.name)
      .distinct() // The scene file contains duplicates.

    catalog.rdd
  }
}


