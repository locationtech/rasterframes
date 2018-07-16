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

package astraea.spark.rasterframes.experimental.datasource.stac

import astraea.spark.rasterframes._
import astraea.spark.rasterframes.rules.SpatialRelationReceiver
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types._

/**
 *
 *
 * @since 7/16/18
 */
@Experimental
case class STACRelation(sqlContext: SQLContext, path: String, filters: Seq[Filter] = Seq.empty) extends BaseRelation
  with PrunedFilteredScan with SpatialRelationReceiver[STACRelation] with LazyLogging {

  val assetSchema = DataTypes.createMapType(StringType, StringType, true)
  override def schema: StructType = StructType(Seq(
    StructField("id", StringType, false),
    StructField("datetime", TimestampType, false),
    StructField("properties", DataTypes.createMapType(StringType, StringType, false)),
    StructField("bbox", extentEncoder.schema, false),
    StructField("geometry", org.apache.spark.sql.jts.JTSTypes.GeometryTypeInstance, false),
    StructField("assets", assetSchema, false)
  ))

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    logger.debug(s"Required columns: ${requiredColumns.mkString(", ")}")
    logger.debug(s"Filters: ${filters.mkString(", ")}")

    /** If no filters are provided, were not going to return the whole catalog. */
    if(requiredColumns.isEmpty || filters.isEmpty) sqlContext.sparkContext.emptyRDD[Row]
    else {
      val columnIndexes = requiredColumns.map(schema.fieldIndex)


      ???
    }
  }

  override def withFilter(filter: Filter): STACRelation =  copy(filters = filters :+ filter)
  override def hasFilter(filter: Filter): Boolean = filters.contains(filter)
}
