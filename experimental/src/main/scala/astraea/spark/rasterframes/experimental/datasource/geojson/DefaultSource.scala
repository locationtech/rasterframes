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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.locationtech.geomesa.spark.jts._
import astraea.spark.rasterframes.datasource._
import org.apache.spark.annotation.Experimental

/**
 * Basic support for parsing GeoJson into a DataFrame with a geometry/spatial column.
 * Properties as rendered as a `Map[String,String]`
 *
 * @since 5/2/18
 */
@Experimental
class DefaultSource extends DataSourceRegister with RelationProvider {
  override def shortName(): String = DefaultSource.SHORT_NAME

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val path = parameters.getOrElse(DefaultSource.PATH_PARAM,
      throw new IllegalArgumentException("Valid URI 'path' parameter required.")
    )
    sqlContext.withJTS
    GeoJsonRelation(sqlContext, path)
  }
}

object DefaultSource {
  final val SHORT_NAME = "geojson"
  final val PATH_PARAM = "path"

}
