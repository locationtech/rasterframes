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

package org.locationtech.rasterframes.experimental.datasource.awspds

import java.io.FileNotFoundException
import java.net.URI

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.locationtech.rasterframes.experimental.datasource.ResourceCacheSupport

/**
 * Data source for querying AWS PDS catalog of L8 imagery.
 *
 * @since 9/28/17
 */
class L8CatalogDataSource extends DataSourceRegister with RelationProvider {
  def shortName = L8CatalogDataSource.SHORT_NAME

  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    require(parameters.get("path").isEmpty, "L8CatalogDataSource doesn't support specifying a path. Please use `load()`.")

    val conf = sqlContext.sparkContext.hadoopConfiguration
    implicit val fs = FileSystem.get(conf)
    val path = L8CatalogDataSource.sceneListFile
    L8CatalogRelation(sqlContext, path)
  }
}

object L8CatalogDataSource extends ResourceCacheSupport {
  final val SHORT_NAME: String = "aws-pds-l8-catalog"
  private val remoteSource = URI.create("http://landsat-pds.s3.amazonaws.com/c1/L8/scene_list.gz")
  private def sceneListFile(implicit fs: FileSystem) =
    cachedURI(remoteSource).getOrElse(throw new FileNotFoundException(remoteSource.toString))
}


