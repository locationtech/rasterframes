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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

/**
 * Experiment in providing RasterFrames over whole PDS catalog.
 *
 * @since 8/21/18
 */
class L8DataSource extends DataSourceRegister with RelationProvider {
  override def shortName(): String = L8DataSource.SHORT_NAME
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = L8Relation(sqlContext)
}

object L8DataSource {
  final val SHORT_NAME = "awsl8"
}
