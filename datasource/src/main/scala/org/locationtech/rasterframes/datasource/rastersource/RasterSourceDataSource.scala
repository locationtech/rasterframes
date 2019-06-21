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

package org.locationtech.rasterframes.datasource.rastersource

import org.locationtech.rasterframes._
import org.locationtech.rasterframes.util._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.locationtech.rasterframes.model.TileDimensions

class RasterSourceDataSource extends DataSourceRegister with RelationProvider {
  import RasterSourceDataSource._
  override def shortName(): String = SHORT_NAME
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val bands = parameters.bandIndexes
    val tiling = parameters.tileDims
    val spec = parameters.pathSpec
    RasterSourceRelation(sqlContext, spec, bands, tiling)
  }
}
object RasterSourceDataSource {
  final val SHORT_NAME = "rastersource"
  final val PATH_PARAM = "path"
  final val PATHS_PARAM = "paths"
  final val BAND_INDEXES_PARAM = "bandIndexes"
  final val TILE_DIMS_PARAM = "tileDimensions"
  final val PATH_TABLE_PARAM = "pathTable"
  final val PATH_TABLE_COL_PARAM = "pathTableColumns"

  final val DEFAULT_COLUMN_NAME = PROJECTED_RASTER_COLUMN.columnName

  trait WithBandColumns {
    def bandColumnNames: Seq[String]
  }
  /** Container for specifying raster paths. */
  case class BandSet(bandPaths: String*)
  case class RasterSourcePathTable(sceneRows: Seq[BandSet], bandColumnNames: String*) extends WithBandColumns {
    require(sceneRows.forall(_.bandPaths.length == bandColumnNames.length),
      "Each scene row must have the same number of entries as band column names")
  }
  /** Container for specifying where to select raster paths from. */
  case class RasterSourcePathTableRef(tableName: String, bandColumnNames: String*) extends WithBandColumns

  private[rastersource]
  implicit class ParamsDictAccessors(val parameters: Map[String, String]) extends AnyVal {

    def tileDims: Option[TileDimensions] =
      parameters.get(TILE_DIMS_PARAM)
        .map(_.split(',').map(_.trim.toInt))
        .map { case Array(cols, rows) => TileDimensions(cols, rows)}

    def bandIndexes: Seq[Int] = parameters
      .get(BAND_INDEXES_PARAM)
      .map(_.split(',').map(_.trim.toInt).toSeq)
      .getOrElse(Seq(0))

    def filePaths: Option[RasterSourcePathTable] = {
      val paths = (
        parameters
          .get(PATHS_PARAM)
          .toSeq
          .flatMap(_.split(Array('\n','\r'))) ++
          parameters
            .get(RasterSourceDataSource.PATH_PARAM)
            .toSeq
        ).filter(_.nonEmpty)

      if (paths.isEmpty) None
      else
        Some(RasterSourcePathTable(paths.map(BandSet(_)), DEFAULT_COLUMN_NAME))
    }

    def pathTable: Option[RasterSourcePathTableRef] = parameters
      .get(PATH_TABLE_PARAM)
      .zip(parameters.get(PATH_TABLE_COL_PARAM))
      .map(p => RasterSourcePathTableRef(p._1, p._2.split(','): _*))
      .headOption

    def pathSpec: Either[RasterSourcePathTable, RasterSourcePathTableRef] = {
      (filePaths, pathTable) match {
        case (Some(f), None) => Left(f)
        case (None, Some(p)) => Right(p)
        case _ => throw new IllegalArgumentException("Only one of a set of file paths OR a paths table column may be provided.")
      }
    }
  }
}
