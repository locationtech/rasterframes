/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Astraea, Inc.
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
 */

package astraea.spark.rasterframes.sources

import java.net.URI

import geotrellis.spark._
import geotrellis.vector.Extent
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql._
import org.apache.spark.sql.sources._

@Experimental
class GeoTrellisDataSource extends DataSourceRegister with RelationProvider {
  def shortName(): String = "geotrellis"

  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    require(parameters.contains("uri"), "'uri' parameter for catalog location required.")
    require(parameters.contains("layer"), "'layer' parameter for raster layer name required.")
    require(parameters.contains("zoom"), "'zoom' parameter for raster layer zoom level required.")

    gt.gtRegister(sqlContext)

    val uri: URI = URI.create(parameters("uri"))
    val layerId: LayerId = LayerId(parameters("layer"), parameters("zoom").toInt)
    val bbox: Option[Extent] = parameters.get("bbox").map(Extent.fromString)

    // here would be the place to read the layer metadata
    // and dispatch based on key type to spatial or spacetime relation
    GeoTrellisRelation(sqlContext, uri, layerId, bbox)
  }
}
