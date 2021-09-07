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

import geotrellis.vector.Extent
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SQLContext, TypedColumn}
import org.locationtech.rasterframes.encoders.SparkBasicEncoders.stringEnc
import org.locationtech.rasterframes.experimental.datasource.CachedDatasetRelation
/**
 * Schema definition and parser for AWS PDS L8 scene data.
 *
 * @author sfitch
 * @since 9/28/17
 */
case class L8CatalogRelation(sqlContext: SQLContext, sceneListPath: HadoopPath)
  extends BaseRelation with TableScan with CachedDatasetRelation {
  import L8CatalogRelation._

  override def schema: StructType = L8CatalogRelation.schema

  protected def cacheFile: HadoopPath = sceneListPath.suffix(".parquet")

  protected def constructDataset: Dataset[Row] = {
    import org.locationtech.rasterframes.encoders.StandardEncoders.extentEncoder
    import sqlContext.implicits._
    logger.debug("Parsing " + sceneListPath)

    val bandCols = Bands.values.toSeq.map(b => l8_band_url(b) as b.toString)

    sqlContext.read
      .schema(inputSchema)
      .option("header", "true")
      .csv(sceneListPath.toString)
      .withColumn("__url",  regexp_replace($"download_url", "index.html", ""))
      .where(not($"${PRODUCT_ID.name}".endsWith("RT")))
      .drop("download_url")
      .withColumn(BOUNDS_WGS84.name, struct(
        $"min_lon" as "xmin",
        $"min_lat" as "ymin",
        $"max_lon" as "xmax",
        $"max_lat" as "ymax"
      ).as[Extent])
      .withColumnRenamed("__url", DOWNLOAD_URL.name)
      .select(col("*") +: bandCols: _*)
      .select(schema.map(f => col(f.name)): _*)
      .orderBy(ACQUISITION_DATE.name, PATH.name, ROW.name)
      .distinct() // The scene file contains duplicates.
      .repartition(defaultNumPartitions, col(PATH.name), col(ROW.name))


  }
}

object L8CatalogRelation extends PDSFields {

  /**
    * Constructs link with the form:
    * `https://s3-us-west-2.amazonaws.com/landsat-pds/c1/L8/149/039/LC08_L1TP_149039_20170411_20170415_01_T1/LC08_L1TP_149039_20170411_20170415_01_T1_{bandId].TIF`
    * @param band Band identifier
    * @return
    */
  def l8_band_url(band: Bands.Band): TypedColumn[Any, String] = {
    concat(col("download_url"), concat(col("product_id"), lit(s"_$band.TIF")))
  }.as(band.toString).as[String]

  private def inputSchema = StructType(Seq(
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

  object Bands extends Enumeration {
    type Band = Value
    val B1, B2, B3, B4, B5, B6, B7, B8, B9, B10, B11, BQA = Value
    val names: Seq[String] = values.toSeq.map(_.toString)
  }


  def schema = StructType(Seq(
    PRODUCT_ID,
    ENTITY_ID,
    ACQUISITION_DATE,
    CLOUD_COVER,
    PROC_LEVEL,
    PATH,
    ROW,
    BOUNDS_WGS84
  ) ++ Bands.names.map(n => StructField(n, StringType, true)))
}


