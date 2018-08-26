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

import astraea.spark.rasterframes.StandardColumns
import astraea.spark.rasterframes.util._
import astraea.spark.rasterframes.rules._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.gt.types.TileUDT
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Spark relation over AWS PDS Landsat 8 collection.
 *
 * @since 8/21/18
 */
case class L8Relation(sqlContext: SQLContext, filters: Seq[Filter] = Seq.empty)
  extends BaseRelation with PrunedFilteredScan with SpatialRelationReceiver[L8Relation]  with LazyLogging {
  override def schema: StructType = L8Relation.schema

  /** Create new relation with the give filter added. */
  override def withFilter(value: Filter): L8Relation = copy(filters = filters :+ value)

  /** Check to see if relation already exists in this. */
  override def hasFilter(filter: Filter): Boolean = filters.contains(filter)

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    val TS = PDSFields.TIMESTAMP.name
    super.unhandledFilters(filters).filterNot {
      case GreaterThan(TS, _) ⇒ true
      case GreaterThanOrEqual(TS, _) ⇒ true
      case LessThan(TS, _) ⇒ true
      case LessThanOrEqual(TS, _) ⇒ true
      case _ ⇒ false
    }
  }

  override def buildScan(requiredColumns: Array[String], sparkFilters: Array[Filter]): RDD[Row] = {
    logger.debug(s"Required columns: ${requiredColumns.mkString(", ")}")
    val aggFilters = sparkFilters.toSet.union(splitFilters(filters).toSet)
    logger.debug(s"Filters: $aggFilters")

    sqlContext.emptyDataFrame.rdd
  }
}

object L8Relation extends PDSFields {
  object Bands extends Enumeration {
    type Bands = Value
    val B1, B2, B3, B4, B5, B6, B7, B8, B9, B10, B11, BQA = Value
  }

  lazy val schema: StructType = {
    val tileType = new TileUDT()
    StructType(
      L8CatalogRelation.schema.collect {
        case ACQUISITION_DATE ⇒ ACQUISITION_DATE.copy(name = StandardColumns.TIMESTAMP_COLUMN.columnName)
        case s if s.name == BOUNDS_WGS84.name ⇒ BOUNDS
        case s if s != DOWNLOAD_URL ⇒ s
      } ++ L8Relation.Bands.values.toSeq.map(b ⇒ StructField(b.toString, tileType, true))
    )
  }
}
