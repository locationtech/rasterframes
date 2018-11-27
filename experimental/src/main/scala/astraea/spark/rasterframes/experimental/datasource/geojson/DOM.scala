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

import com.vividsolutions.jts.geom.{Envelope, Geometry}
import com.vividsolutions.jts.io.geojson.{GeoJsonReader, GeoJsonWriter}
import geotrellis.vector.Extent
import spray.json._
import spray.json.DefaultJsonProtocol._

/**
 * Lightweight DOM for parsing GeoJSON feature sets.
 *
 * @since 7/17/18
 */
object DOM {
  case class GeoJsonFeatureSet(features: Seq[GeoJsonFeature])
  object GeoJsonFeatureSet {
    implicit val domFormat: RootJsonFormat[GeoJsonFeatureSet] =  jsonFormat1(GeoJsonFeatureSet.apply)
  }

  case class GeoJsonFeature(geometry: Geometry, bbox: Option[Extent], properties: Map[String, JsValue])
  object GeoJsonFeature {
    implicit val featureFormat: RootJsonFormat[GeoJsonFeature] = jsonFormat3(GeoJsonFeature.apply)
  }

  implicit val geomFormat: RootJsonFormat[Geometry] = new RootJsonFormat[Geometry] {
    def read(json: JsValue): Geometry = {
      val reader = new GeoJsonReader()
      reader.read(json.compactPrint)
    }
    def write(obj: Geometry): JsValue = {
      val writer = new GeoJsonWriter()
      writer.write(obj).parseJson
    }
  }

  implicit val envelopeFormat: RootJsonFormat[Envelope] = new RootJsonFormat[Envelope] {
    override def read(json: JsValue): Envelope = json match {
      // Per the STAC spect, bbox values are in the order [west, south, east, north]
      case JsArray(Vector(JsNumber(west), JsNumber(south), JsNumber(east), JsNumber(north))) ⇒
        new Envelope(west.toDouble, east.toDouble, south.toDouble, north.toDouble)
      case x => deserializationError("Expected Array as JsArray, but got " + x)
    }

    override def write(obj: Envelope): JsValue = JsArray(Vector(
      JsNumber(obj.getMinX), JsNumber(obj.getMinY), JsNumber(obj.getMaxX), JsNumber(obj.getMaxY)))
  }

  implicit val extentFormat: RootJsonFormat[Extent] = new RootJsonFormat[Extent] {
    override def read(json: JsValue): Extent = json match {
      // Per the STAC spect, bbox values are in the order [west, south, east, north]
      case JsArray(Vector(JsNumber(west), JsNumber(south), JsNumber(east), JsNumber(north))) ⇒
        Extent(west.toDouble, south.toDouble, east.toDouble, north.toDouble)
      case x => deserializationError("Expected Array as JsArray, but got " + x)
    }

    override def write(obj: Extent): JsValue = JsArray(Vector(
      JsNumber(obj.xmin), JsNumber(obj.ymin), JsNumber(obj.xmax), JsNumber(obj.ymax)))
  }
}
