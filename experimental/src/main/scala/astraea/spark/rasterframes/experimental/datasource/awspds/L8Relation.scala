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

import astraea.spark.rasterframes._
import astraea.spark.rasterframes.experimental.datasource._
import astraea.spark.rasterframes.experimental.datasource.awspds.L8Relation.Bands
import astraea.spark.rasterframes.rules.SpatialFilters.Intersects
import astraea.spark.rasterframes.rules._
import astraea.spark.rasterframes.util._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.gt.types.TileUDT
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row, SQLContext}

/**
 * Spark relation over AWS PDS Landsat 8 collection.
 *
 * @since 8/21/18
 */
case class L8Relation(sqlContext: SQLContext, useTiling: Boolean, filters: Seq[Filter] = Seq.empty)
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

  // TODO: Is there a more clean, direct way of delegating filtering other
  // TODO: having to reconstitute the predicates like this?
  private def colExpr(filter: Filter): Column = filter match {
    case GreaterThan(name, value) ⇒ col(name) > value
    case GreaterThanOrEqual(name, value) ⇒ col(name) >= value
    case LessThan(name, value) ⇒ col(name) < value
    case LessThanOrEqual(name, value) ⇒ col(name) <= value
    case EqualTo(name, value) ⇒ col(name) === value
    case Intersects(name, value) ⇒
      st_intersects(col(name), geomLit(value))
  }

  override def buildScan(requiredColumns: Array[String], sparkFilters: Array[Filter]): RDD[Row] = {
    logger.debug(s"Required columns: ${requiredColumns.mkString(", ")}")
    val aggFilters = (sparkFilters ++ splitFilters(filters)).distinct
    logger.debug(s"Filters: $aggFilters")

    val catalog = sqlContext.read
      .format(L8CatalogDataSource.SHORT_NAME)
      .load()
      .withColumnRenamed(PDSFields.ACQUISITION_DATE.name, PDSFields.TIMESTAMP.name)
      .withColumn(PDSFields.BOUNDS.name, boundsGeometry(col(PDSFields.BOUNDS_WGS84.name)))
      .drop(PDSFields.BOUNDS_WGS84.name)

    val filtered = aggFilters.foldLeft(catalog)((cat, filter) ⇒ cat.where(colExpr(filter)))

    val (bands, other) = requiredColumns.partition(Bands.names.contains)

    filtered.select(other.map(col) :+ raster_ref(bands.map(b ⇒ l8_band_url(b)), useTiling): _*).rdd
  }
}

object L8Relation extends PDSFields {
  object Bands extends Enumeration {
    type Bands = Value
    val B1, B2, B3, B4, B5, B6, B7, B8, B9, B10, B11, BQA = Value
    val names = values.toSeq.map(_.toString)
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
