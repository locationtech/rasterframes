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
import astraea.spark.rasterframes.model.TileDimensions
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

class RasterSourceDataSource extends DataSourceRegister with RelationProvider {
  import RasterSourceDataSource._
  override def shortName(): String = SHORT_NAME
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val inexes = parameters.bandIndexes
    val files = parameters.filePaths
    val tiling = parameters.tileDims
    RasterSourceRelation(sqlContext, files, inexes, tiling)
  }
}
object RasterSourceDataSource {
  final val SHORT_NAME = "rastersource"
  final val PATH_PARAM = "path"
  final val PATHS_PARAM = "paths"
  final val BAND_INDEXES_PARAM = "bandIndexes"
  final val TILE_DIMS_PARAM = "tileDimensions"

  private[rastersource]
  implicit class ParamsDictAccessors(val parameters: Map[String, String]) extends AnyVal {
    def filePaths: Seq[String] = (
      parameters
        .get(PATHS_PARAM)
        .toSeq
        .flatMap(_.split(Array('\n','\r'))) ++
        parameters
          .get(RasterSourceDataSource.PATH_PARAM)
          .toSeq
      )
      .filter(_.nonEmpty)

    def tileDims: Option[TileDimensions] =
      parameters.get(TILE_DIMS_PARAM)
      .map(_.split(',').map(_.trim.toInt))
      .map { case Array(cols, rows) => TileDimensions(cols, rows)}

    def bandIndexes: Seq[Int] = parameters
      .get(BAND_INDEXES_PARAM)
      .map(_.split(',').map(_.trim.toInt).toSeq)
      .getOrElse(Seq(0))
  }
}
