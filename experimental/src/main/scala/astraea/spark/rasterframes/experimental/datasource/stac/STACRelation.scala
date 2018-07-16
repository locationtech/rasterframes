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

import java.net.{URI, URL}
import java.time.{LocalDate, ZonedDateTime}

import astraea.spark.rasterframes._
import astraea.spark.rasterframes.experimental.datasource.DownloadSupport
import astraea.spark.rasterframes.experimental.datasource.stac.STACRelation.C
import astraea.spark.rasterframes.rules.TemporalFilters.{BetweenDates, BetweenTimes}
import astraea.spark.rasterframes.rules.SpatialRelationReceiver
import com.typesafe.scalalogging.LazyLogging
import org.apache.http.client.utils.URIBuilder
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types._
import spray.json.{JsArray, JsObject}


/**
 * Table relation over a STAC ReST API
 *
 * @since 7/16/18
 */
@Experimental
case class STACRelation(sqlContext: SQLContext, base: URI, filters: Seq[Filter] = Seq.empty) extends BaseRelation
  with PrunedFilteredScan with SpatialRelationReceiver[STACRelation] with DownloadSupport with LazyLogging {

  val assetSchema = DataTypes.createMapType(StringType, StringType, true)
  override def schema: StructType = StructType(Seq(
    StructField(C.ID, StringType, false),
    StructField(C.DATE_TIME, TimestampType, false),
    StructField(C.PROPERTIES, DataTypes.createMapType(StringType, StringType, false)),
    StructField(C.BBOX, extentEncoder.schema, false),
    StructField(C.GEOMETRY, org.apache.spark.sql.jts.JTSTypes.GeometryTypeInstance, false),
    StructField(C.ASSETS, assetSchema, false)
  ))

  override def buildScan(requiredColumns: Array[String], attributeFilters: Array[Filter]): RDD[Row] = {
    // Combine filter sets, taking care of duplicates.
    val activeFilters = filters ++ attributeFilters.filterNot(_.references.contains(C.DATE_TIME))

    logger.debug(s"Required columns: ${requiredColumns.mkString(", ")}")
    logger.debug(s"Filters:\n\t${activeFilters.mkString(", ")}")

    /** If no filters are provided, were not going to return the whole catalog. */
    if(requiredColumns.isEmpty || filters.isEmpty) sqlContext.sparkContext.emptyRDD[Row]
    else {
      val columnIndexes = requiredColumns.map(schema.fieldIndex)

      val query = STACRelation.toUrl(base, activeFilters)

      val result = getJson(query)

      result.asJsObject.fields("features") match {
        case JsArray(features) ⇒ features.map(_.asJsObject)
      }
      ???
    }
  }

  override def withFilter(filter: Filter): STACRelation =  copy(filters = filters :+ filter)
  override def hasFilter(filter: Filter): Boolean = filters.contains(filter)
}

object STACRelation {
  object C {
    final val ID = "id"
    final val DATE_TIME = "datetime"
    final val PROPERTIES = "properties"
    final val BBOX = "bbox"
    final val GEOMETRY = "geometry"
    final val ASSETS = "assets"
  }

  def toQueryParam(f: Filter): Option[(String, String)] = f match {
    case BetweenTimes(C.DATE_TIME, start, end) ⇒
      val s = start.toLocalDateTime.toLocalDate
      val e = end.toLocalDateTime.toLocalDate
      Some((C.DATE_TIME, s"$s/$e"))
    case BetweenDates(C.DATE_TIME, start, end) ⇒
      val s = start.toLocalDate
      val e = end.toLocalDate
      Some((C.DATE_TIME, s"$s/$e"))
    case _ ⇒ None
  }
  def toUrl(base: URI, filters: Seq[Filter]): URI = {
    val builder = filters
      .flatMap(toQueryParam)
      .foldLeft(new URIBuilder(base))((b, p)⇒ b.addParameter(p._1, p._2))
    builder.build()
  }
}
