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

package org.locationtech.rasterframes.datasource.geotrellis

import java.net.URI

import geotrellis.store._
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.locationtech.rasterframes.datasource.geotrellis.GeoTrellisCatalog.GeoTrellisCatalogRelation

/**
 *
 * @since 1/12/18
 */
@Experimental
class GeoTrellisCatalog extends DataSourceRegister with RelationProvider {
  def shortName() = "geotrellis-catalog"

  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    require(parameters.contains("path"), "'path' parameter required.")
    val uri: URI = URI.create(parameters("path"))
    GeoTrellisCatalogRelation(sqlContext, uri)
  }
}

object GeoTrellisCatalog {

  case class GeoTrellisCatalogRelation(sqlContext: SQLContext, uri: URI) extends BaseRelation with TableScan {
    import sqlContext.implicits._

    private lazy val attributes = AttributeStore(uri)

    // NB: It is expected that the number of layers is going to be small enough that
    // doing all this in-core generation of the catalog table will be negligible.
    // If this becomes a problem then re-write it starting off with an RDD of layerIds
    // and flow from there.
    private lazy val layers = {
      // The attribute groups are processed separately and joined at the end to
      // maintain a semblance of separation in the resulting schema.
      val mergeId = (id: Int, json: io.circe.JsonObject) => {
        import io.circe.syntax._
        val jid = id.asJson
        json.add("index", jid).asJson
      }

      implicit val layerStuffEncoder: Encoder[(Int, Layer)] = Encoders.tuple(
        Encoders.scalaInt, layerEncoder
      )

      val layerIds = attributes.layerIds

      val layerSpecs = layerIds.zipWithIndex.map {
        case (id, index) => (index: Int, Layer(uri, id))
      }

      val indexedLayers = layerSpecs
        .toDF("index", "layer")

      val headerRows = layerSpecs
        .map{case (index, layer) =>(index, attributes.readHeader[io.circe.JsonObject](layer.id))}
        .map(mergeId.tupled)
        .map(io.circe.Printer.noSpaces.print)
        .toDS

      val metadataRows = layerSpecs
        .map{case (index, layer) => (index, attributes.readMetadata[io.circe.JsonObject](layer.id))}
        .map(mergeId.tupled)
        .map(io.circe.Printer.noSpaces.print)
        .toDS


      val headers = sqlContext.read.json(headerRows)
      val metadata = sqlContext.read.json(metadataRows)

      broadcast(indexedLayers).join(broadcast(headers), Seq("index")).join(broadcast(metadata), Seq("index"))
    }

    def schema: StructType = layers.schema

    def buildScan(): RDD[Row] = layers.rdd
  }
}
