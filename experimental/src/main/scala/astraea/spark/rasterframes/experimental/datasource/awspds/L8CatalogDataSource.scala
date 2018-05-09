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


import java.io.FileNotFoundException
import java.net.URI

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

/**
 * Data source for querying AWS PDS catalog of L8 imagery.
 *
 * @since 9/28/17
 */
class L8CatalogDataSource extends DataSourceRegister with RelationProvider {
  def shortName = L8CatalogDataSource.NAME

  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val path = parameters.getOrElse("path", L8CatalogDataSource.sceneListFile.toUri.toASCIIString)
    L8CatalogRelation(sqlContext, path)
  }
}

object L8CatalogDataSource extends LazyLogging with ResourceCacheSupport {
  final val NAME: String = "awsl8-catalog"
  private val remoteSource = URI.create("http://landsat-pds.s3.amazonaws.com/c1/L8/scene_list.gz")
  private lazy val sceneListFile =
    cachedURI(remoteSource).getOrElse(throw new FileNotFoundException(remoteSource.toString))
}


