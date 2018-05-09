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

package astraea.spark.rasterframes.experimental.datasource.geojson

import java.net.URI

import astraea.spark.rasterframes.experimental.datasource.geojson.GeoJsonRelation._
import com.vividsolutions.jts.io.geojson.GeoJsonReader
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.jts.JTSTypes
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{DataTypes, StringType, StructField, StructType}
import spray.json.DefaultJsonProtocol._
import spray.json._

/**
 * Basic support for parsing GeoJson into a DataFrame with a geometry/spatial column.
 * Properties as rendered as a `Map[String,String]`.
 *
 * @since 5/2/18
 */
@Experimental
case class GeoJsonRelation(sqlContext: SQLContext, path: String) extends BaseRelation with TableScan {
  override def schema: StructType = StructType(Seq(
    StructField(GEOMETRY_COLUMN, JTSTypes.GeometryTypeInstance, false),
    StructField(PROPERTIES_COLUMN,DataTypes.createMapType(StringType, StringType, true), true)
  ))

  override def buildScan(): RDD[Row] = {
    val sc = sqlContext.sparkContext
    sc.wholeTextFiles(path).mapPartitions { iter ⇒
      val reader = new GeoJsonReader()
      for {
        (_, text) ← iter
        dom = text.parseJson.convertTo[GeoJsonDom]
        feature ← dom.features
        geom = reader.read(feature.geometry.compactPrint)
        props = feature.properties.mapValues(_.toString)
      } yield Row(geom, props)
    }
  }
}

object GeoJsonRelation {
  final val GEOMETRY_COLUMN = "geometry"
  final val PROPERTIES_COLUMN = "properties"

  case class GeoJsonDom(features: Seq[GeoJsonFeature])
  case class GeoJsonFeature(geometry: JsValue, properties: Map[String, JsValue])

  implicit val featureFormat: RootJsonFormat[GeoJsonFeature] =  jsonFormat2(GeoJsonFeature)
  implicit val domFormat: RootJsonFormat[GeoJsonDom] =  jsonFormat1(GeoJsonDom)
}
