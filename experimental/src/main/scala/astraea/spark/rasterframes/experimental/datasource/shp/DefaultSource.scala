/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2019 Astraea, Inc.
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

package astraea.spark.rasterframes.experimental.datasource.shp
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

/**
 *
 *
 * @since 2019-01-05
 */
@Experimental
class DefaultSource extends DataSourceRegister with RelationProvider {
  import DefaultSource._
  override def shortName(): String = SHORT_NAME
  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String]): BaseRelation = {
    val path = parameters.getOrElse(
      PATH_PARAM,
      throw new IllegalArgumentException("Valid URI 'path' parameter required."))
    ShapeFileRelation(sqlContext, path)
  }
}
object DefaultSource {
  final val SHORT_NAME = "shp"
  final val PATH_PARAM = "path"
}
