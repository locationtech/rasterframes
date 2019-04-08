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

package astraea.spark.rasterframes.datasource.rastersource
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

class RasterSourceDataSource extends DataSourceRegister with RelationProvider {
  override def shortName(): String = RasterSourceDataSource.SHORT_NAME
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val bandCount = parameters
      .get(RasterSourceDataSource.BAND_COUNT_PARAM)
      .map(_.toInt)
      .getOrElse(1)

    val files = RasterSourceDataSource.filePaths(parameters)

    RasterSourceRelation(sqlContext, files, bandCount)
  }
}
object RasterSourceDataSource {
  final val SHORT_NAME = "rastersource"
  final val PATH_PARAM = "path"
  final val PATHS_PARAM = "paths"
  final val BAND_COUNT_PARAM = "bandCount"

  private[rastersource]
  def filePaths(parameters: Map[String, String]): Seq[String] =
    (
      parameters
        .get(RasterSourceDataSource.PATHS_PARAM)
        .toSeq
        .flatMap(_.split(Array('\n','\r'))) ++
        parameters
          .get(RasterSourceDataSource.PATH_PARAM)
          .toSeq
      )
      .filter(_.nonEmpty)

}
