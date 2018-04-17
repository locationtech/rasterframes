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

import java.net.URL

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

/**
 * https://docs.opendata.aws/modis-pds/readme.html
 *
 * @since 5/4/18
 */
class MODISCatalogDataSource extends DataSourceRegister with RelationProvider {
  override def shortName(): String = MODISCatalogDataSource.NAME

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val path = parameters.getOrElse("path", MODISCatalogDataSource.sceneListFile.toUri.toASCIIString)
    MODISCatalogRelation(sqlContext, path)
  }
}

object MODISCatalogDataSource extends LazyLogging with SceneFileCacheSupport {
  val NAME = "modis-catalog"
  protected val remoteSource = new URL("https://modis-pds.s3.amazonaws.com/MCD43A4.006/2013-01-03_scenes.txt")
  private lazy val sceneListFile = sceneFile(remoteSource)
}
