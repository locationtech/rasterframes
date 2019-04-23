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

<<<<<<< HEAD:datasource/src/main/scala/org/locationtech/rasterframes/datasource/rastersource/RasterSourceRelation.scala
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.datasource.rastersource.RasterSourceRelation.bandNames
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.expressions.transformers.{RasterRefToTile, RasterSourceToRasterRefs, URIToRasterSource}
import org.locationtech.rasterframes.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.locationtech.rasterframes.model.TileDimensions
import org.locationtech.rasterframes.tiles.ProjectedRasterTile
=======
import astraea.spark.rasterframes._
import astraea.spark.rasterframes.datasource.rastersource.RasterSourceDataSource.PathColumn
import astraea.spark.rasterframes.datasource.rastersource.RasterSourceRelation.bandNames
import astraea.spark.rasterframes.encoders.CatalystSerializer._
import astraea.spark.rasterframes.expressions.transformers.{RasterRefToTile, RasterSourceToRasterRefs, URIToRasterSource}
import astraea.spark.rasterframes.model.TileDimensions
import astraea.spark.rasterframes.tiles.ProjectedRasterTile
import astraea.spark.rasterframes.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions.col
>>>>>>> Added ability to use a RasterSourceRelation against a table/view of paths.:datasource/src/main/scala/astraea/spark/rasterframes/datasource/rastersource/RasterSourceRelation.scala

/**
  * Constructs a Spark Relation over one or more RasterSource paths.
  * @param sqlContext
  * @param paths list of URIs to fetch rastefrom.
  * @param bandIndexes band indexes to fetch
  * @param subtileDims how big to tile/subdivide rasters info
  */
case class RasterSourceRelation(sqlContext: SQLContext, paths: Seq[String], pathTable: Option[PathColumn], bandIndexes: Seq[Int], subtileDims: Option[TileDimensions]) extends BaseRelation with TableScan {

  override def schema: StructType = StructType(Seq(
    StructField(PATH_COLUMN.columnName, StringType, false)
  ) ++ {
    val tileSchema = schemaOf[ProjectedRasterTile]
    for {
      name <- bandNames(bandIndexes)
    } yield StructField(name, tileSchema, true)
  })

  override def buildScan(): RDD[Row] = {
    import sqlContext.implicits._
    val names = bandNames(bandIndexes)
    val refs = RasterSourceToRasterRefs(subtileDims, bandIndexes, URIToRasterSource($"path"))
    val refsToTiles = names.map(n => RasterRefToTile($"$n") as n)

    val pathsTable: DataFrame = pathTable match {
      case Some(spec) =>
        sqlContext.table(spec.tableName).select(col(spec.columnName) as "path")
      case _ => paths.toDF("path")
    }

    val df = pathsTable
      .select(PATH_COLUMN, refs as names)
      .select(PATH_COLUMN +: refsToTiles: _*)
    df.rdd
  }
}
object RasterSourceRelation {
  private def bandNames(bandIndexes: Seq[Int]): Seq[String] = bandIndexes match {
    case Seq() => Seq.empty
    case Seq(0) => Seq(TILE_COLUMN.columnName)
    case s =>
      val basename = TILE_COLUMN.columnName
      s.map(n => basename + "_b" + n)
  }
}
