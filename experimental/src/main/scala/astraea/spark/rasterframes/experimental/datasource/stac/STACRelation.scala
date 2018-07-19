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

import java.net.URI
import java.sql.{Date, Timestamp}
import java.time.ZonedDateTime

import astraea.spark.rasterframes
import astraea.spark.rasterframes._
import astraea.spark.rasterframes.datasource.splitFilters
import astraea.spark.rasterframes.experimental.datasource.DownloadSupport
import astraea.spark.rasterframes.experimental.datasource.geojson.DOM
import astraea.spark.rasterframes.experimental.datasource.stac.STACRelation._
import astraea.spark.rasterframes.rules.SpatialFilters.Intersects
import astraea.spark.rasterframes.rules.SpatialRelationReceiver
import astraea.spark.rasterframes.rules.TemporalFilters.{BetweenDates, BetweenTimes}
import astraea.spark.rasterframes.util._
import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Point
import org.apache.http.client.utils.URIBuilder
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.util.Try
import scala.util.control.NonFatal

/**
 * Table relation over a STAC ReST API
 *
 * @since 7/16/18
 */
@Experimental
case class STACRelation(sqlContext: SQLContext, base: URI, numPartitions: Option[Int], filters: Seq[Filter] = Seq.empty) extends BaseRelation
  with PrunedFilteredScan with SpatialRelationReceiver[STACRelation] with DownloadSupport with LazyLogging {

  private val assetSchema = DataTypes.createMapType(StringType, StringType, true)
  private val geomSchema = org.apache.spark.sql.jts.JTSTypes.GeometryTypeInstance

  override def schema: StructType = StructType(Seq(
    StructField(C.ID, StringType, false),
    StructField(C.TIMESTAMP, TimestampType, false),
    StructField(C.PROPERTIES, DataTypes.createMapType(StringType, StringType, false)),
    StructField(C.BOUNDS, geomSchema, true),
    StructField(C.GEOMETRY, geomSchema, false),
    StructField(C.ASSETS, assetSchema, false)
  ))

  override def buildScan(requiredColumns: Array[String], attributeFilters: Array[Filter]): RDD[Row] = {
    // Combine filter sets, taking care of duplicates.
    val activeFilters = filters ++ attributeFilters.filterNot(_.references.contains(C.TIMESTAMP))

    logger.debug(s"Required columns: ${requiredColumns.mkString(", ")}")
    logger.debug(s"Filters:\n\t${activeFilters.mkString(", ")}")

    /** If no filters are provided, were not going to return the whole catalog. */
    if(filters.isEmpty) sqlContext.sparkContext.emptyRDD[Row]
    else {
      val query = STACRelation.toUrl(base, activeFilters)

      val result = getJson(query)

      val parts = numPartitions.getOrElse(
        sqlContext.getConf("spark.default.parallelism").toInt
      )

      val featureRDD = result.asJsObject.fields("features") match {
        case JsArray(features) ⇒ sqlContext.sparkContext.makeRDD(features).repartition(parts)
        case _ ⇒ throw new IllegalArgumentException("Unexpected JSON response recevied:\n" + result)
      }

      featureRDD.map(_.convertTo[DOM.GeoJsonFeature]).map { feature ⇒
        val entries = requiredColumns.map {
          case C.ID ⇒ feature.properties(C.ID).convertTo[String]
          case C.TIMESTAMP ⇒ feature.properties("datetime").convertTo[Timestamp]
          case C.PROPERTIES ⇒ feature.properties.mapValues(_.toString)
          case C.BOUNDS ⇒ feature.bbox.map(_.jtsGeom).orNull
          case C.GEOMETRY ⇒ feature.geometry
          case C.ASSETS ⇒ feature.assets.mapValues(_.toString)
        }
        Row(entries: _*)
      }
    }
  }

  override def withFilter(filter: Filter): STACRelation =  copy(filters = filters :+ filter)
  override def hasFilter(filter: Filter): Boolean = filters.contains(filter)
}

object STACRelation {
  object C {
    final val ID = "id"
    lazy val TIMESTAMP = TIMESTAMP_COLUMN.columnName
    final val PROPERTIES = "properties"
    final val BOUNDS = BOUNDS_COLUMN.columnName
    lazy val GEOMETRY = "geometry"
    final val ASSETS = "assets"
  }

  def toQueryParam(f: Filter): Option[(String, String)] = {
    val DT_PARAM = "datetime"
    val INTR_PARAM = "intersects"
    f match {
      case BetweenTimes(C.TIMESTAMP, start, end) ⇒
        // TODO: is it ok to drop the time component?
        val s = start.toLocalDateTime.toLocalDate
        val e = end.toLocalDateTime.toLocalDate
        Some((DT_PARAM, s"$s/$e"))
      case BetweenDates(C.TIMESTAMP, start, end) ⇒
        val s = start.toLocalDate
        val e = end.toLocalDate
        Some((DT_PARAM, s"$s/$e"))
      // NB: there's no (possible?) distinction between '>' and '>='
      case GreaterThan(C.TIMESTAMP, dt) ⇒ dt match {
        case time: Timestamp ⇒ Some((DT_PARAM, time.toLocalDateTime.toLocalDate.toString))
        case date: Date ⇒ Some((DT_PARAM, date.toLocalDate.toString))
      }
      case GreaterThanOrEqual(C.TIMESTAMP, dt) ⇒ dt match {
        case time: Timestamp ⇒ Some((DT_PARAM, time.toLocalDateTime.toLocalDate.toString))
        case date: Date ⇒ Some((DT_PARAM, date.toLocalDate.toString))
      }
      case LessThan(C.TIMESTAMP, _) | LessThanOrEqual(C.TIMESTAMP, _) ⇒
        throw new UnsupportedOperationException("STAC API doesn't support 'less than' searches. Use conjunction of '<=' and '>='.")
      case Intersects(C.BOUNDS, geom) ⇒ geom match {
        case p: Point ⇒ Some(INTR_PARAM,
      }

      case x ⇒
        println(x)
        None
    }
  }

  def toUrl(base: URI, filters: Seq[Filter]): URI = {
    val builder = splitFilters(filters)
      .flatMap(toQueryParam)
      .foldLeft(new URIBuilder(base))((b, p)⇒ b.addParameter(p._1, p._2))
    builder.build()
  }

  implicit val tsReader: JsonReader[Timestamp] = new JsonReader[Timestamp] {
    override def read(json: JsValue): Timestamp = json match {
      case JsString(str) ⇒
        Try(Timestamp.valueOf(str)).recover {
          case NonFatal(_) ⇒
            val zdt = ZonedDateTime.parse(str)
            new Timestamp(zdt.toInstant.toEpochMilli)
        }.get
      case x => deserializationError("Expected timestamp as JsString, but got " + x)
    }
  }
}
