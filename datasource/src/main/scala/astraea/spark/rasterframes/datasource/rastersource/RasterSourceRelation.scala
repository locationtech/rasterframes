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

import astraea.spark.rasterframes._
import astraea.spark.rasterframes.encoders.CatalystSerializer._
import astraea.spark.rasterframes.expressions.transformers.{RasterRefToTile, RasterSourceToRasterRefs, URIToRasterSource}
import astraea.spark.rasterframes.model.TileDimensions
import astraea.spark.rasterframes.tiles.ProjectedRasterTile
import astraea.spark.rasterframes.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

case class RasterSourceRelation(sqlContext: SQLContext, paths: Seq[String], bandCount: Int, subtileDims: Option[TileDimensions]) extends BaseRelation with TableScan {
  override def schema: StructType = if (bandCount == 1) RasterSourceRelation.schema
  else {
    val fields = RasterSourceRelation.schema.fields
    StructType(
      fields.dropRight(1) ++ {
        val base = fields.last
        for (b <- 1 to bandCount) yield base.copy(name = base.name + "_" + b)
      }
    )
  }
  override def buildScan(): RDD[Row] = {
    import sqlContext.implicits._
    paths.toDF("path")
      .withColumn("__rr", RasterSourceToRasterRefs(subtileDims, URIToRasterSource($"path")))
      .select(
        PATH_COLUMN,
        RasterRefToTile($"__rr") as TILE_COLUMN.columnName
      )
      .rdd
  }
}
object RasterSourceRelation {
  def schema: StructType = StructType(Seq(
    StructField(PATH_COLUMN.columnName, StringType, false),
    StructField(TILE_COLUMN.columnName, schemaOf[ProjectedRasterTile], true)
  ))
}
