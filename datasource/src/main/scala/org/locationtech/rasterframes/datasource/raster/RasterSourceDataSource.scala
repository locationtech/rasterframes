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

package org.locationtech.rasterframes.datasource.raster

import org.locationtech.rasterframes._
import org.locationtech.rasterframes.util._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.locationtech.rasterframes.model.TileDimensions

import scala.util.Try

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
  final val SHORT_NAME = "raster"
  final val PATH_PARAM = "path"
  final val PATHS_PARAM = "paths"
  final val BAND_INDEXES_PARAM = "bandIndexes"
  final val TILE_DIMS_PARAM = "tileDimensions"
  final val CATALOG_TABLE_PARAM = "catalogTable"
  final val CATALOG_TABLE_COLS_PARAM = "catalogColumns"
  final val CATALOG_CSV_PARAM = "catalogCSV"

  final val DEFAULT_COLUMN_NAME = PROJECTED_RASTER_COLUMN.columnName

  trait WithBandColumns {
    def bandColumnNames: Seq[String]
  }
  /** Container for specifying raster paths. */
  case class BandSet(bandPaths: String*)
  case class RasterSourceCatalog(sceneRows: Seq[BandSet], bandColumnNames: String*) extends WithBandColumns {
    require(sceneRows.forall(_.bandPaths.length == bandColumnNames.length),
      "Each scene row must have the same number of entries as band column names")
  }

  object RasterSourceCatalog {
    def apply(csv: String, bandColumnNames: String*): Option[RasterSourceCatalog] = Try {
      val lines = csv
        .split(Array('\n','\r'))
        .map(_.trim)
        .filter(_.nonEmpty)

      val header = lines.head.split(',').map(_.trim).toSeq
      val rows = lines.tail.map(_.split(',').map(_.trim)).map(BandSet(_: _*))
      val bands = if (bandColumnNames.isEmpty) header
      else bandColumnNames
      RasterSourceCatalog(rows, bands: _*)
    }.toOption

    def apply(singlebandPaths: Seq[String]): Option[RasterSourceCatalog]  =
      if (singlebandPaths.isEmpty) None
      else Some(RasterSourceCatalog(singlebandPaths.map(BandSet(_)), DEFAULT_COLUMN_NAME))
  }

  /** Container for specifying where to select raster paths from. */
  case class RasterSourceCatalogRef(tableName: String, bandColumnNames: String*) extends WithBandColumns

  private[raster]
  implicit class ParamsDictAccessors(val parameters: Map[String, String]) extends AnyVal {

    def tileDims: Option[TileDimensions] =
      parameters.get(TILE_DIMS_PARAM)
        .map(_.split(',').map(_.trim.toInt))
        .map { case Array(cols, rows) => TileDimensions(cols, rows)}

    def bandIndexes: Seq[Int] = parameters
      .get(BAND_INDEXES_PARAM)
      .map(_.split(',').map(_.trim.toInt).toSeq)
      .getOrElse(Seq(0))

    def catalog: Option[RasterSourceCatalog] = {
      val paths = (
        parameters
          .get(PATHS_PARAM)
          .toSeq
          .flatMap(_.split(Array('\n','\r'))) ++
          parameters
            .get(RasterSourceDataSource.PATH_PARAM)
            .toSeq
        ).filter(_.nonEmpty)

      RasterSourceCatalog(paths)
        .orElse(parameters
          .get(CATALOG_CSV_PARAM)
          .flatMap(RasterSourceCatalog(_, catalogTableCols: _*))
        )
    }

    def catalogTableCols: Seq[String] = parameters
      .get(CATALOG_TABLE_COLS_PARAM)
      .map(_.split(',').toSeq)
      .getOrElse(Seq.empty)

    def catalogTable: Option[RasterSourceCatalogRef] = parameters
      .get(CATALOG_TABLE_PARAM)
      .map(p => RasterSourceCatalogRef(p, catalogTableCols: _*))

    def pathSpec: Either[RasterSourceCatalog, RasterSourceCatalogRef] = {
      val a = catalogTable
      val b = catalogTableCols
      (catalog, catalogTable) match {
        case (Some(f), None) => Left(f)
        case (None, Some(p)) => Right(p)
        case _ => throw new IllegalArgumentException("Only one of a set of file paths OR a paths table column may be provided.")
      }
    }
  }
}
