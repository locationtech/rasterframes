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

package org.locationtech.rasterframes.extensions
import geotrellis.proj4.CRS
import geotrellis.raster._
import geotrellis.raster.reproject.Reproject
import geotrellis.vector.Extent
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.model.TileDimensions
import org.locationtech.rasterframes.util._

import scala.util.Random

object RasterJoin {
  private val projOpts = Reproject.Options.DEFAULT

  // NB: Don't be tempted to make this a `val`. Spark will barf if `withRasterFrames` hasn't been called first.
  def reproject_and_merge = udf((leftExtentEnc: Row, leftCRSEnc: Row, tiles: Seq[Tile], rightExtentEnc: Seq[Row], rightCRSEnc: Seq[Row], leftDimsEnc: Row) => {
    require(tiles.length == rightExtentEnc.length && tiles.length == rightCRSEnc.length, "size mismatch")

    val leftExtent = leftExtentEnc.to[Extent]
    val leftDims = leftDimsEnc.to[TileDimensions]
    val leftCRS = leftCRSEnc.to[CRS]
    val rightExtents = rightExtentEnc.map(_.to[Extent])
    val rightCRSs = rightCRSEnc.map(_.to[CRS])

    val cellType = tiles.map(_.cellType).reduce(_ union _)

    val dest: Tile = ArrayTile.empty(cellType, leftDims.cols, leftDims.rows)
    //is there a GT function to do all this?
    tiles.zip(rightExtents).zip(rightCRSs).map {
      case ((tile, extent), crs) =>
        tile.reproject(extent, crs, leftCRS, projOpts)
    }.foldLeft(dest)((d, t) =>
      d.merge(leftExtent, t.extent, t.tile, projOpts.method)
    )
  }).withName("reproject_and_merge")

  def apply(left: DataFrame, right: DataFrame): DataFrame = {
    val df = apply(left, right, left("extent"), left("crs"), right("extent"), right("crs"))
    df.drop(right("extent")).drop(right("crs"))
  }

  def apply(left: DataFrame, right: DataFrame, leftExtent: Column, leftCRS: Column, rightExtent: Column, rightCRS: Column): DataFrame = {
    apply(left, right, st_intersects(
      st_geometry(leftExtent),
      st_reproject(st_geometry(rightExtent), rightCRS, leftCRS)
    ), leftExtent, leftCRS, rightExtent, rightCRS)
  }
  def apply(left: DataFrame, right: DataFrame, joinExprs: Column, leftExtent: Column, leftCRS: Column, rightExtent: Column, rightCRS: Column): DataFrame = {
    // Unique id for temporary columns
    val id = Random.alphanumeric.take(5).mkString("_", "", "_")

    // Post aggregation left extent. We preserve the original name.
    val leftExtent2 = leftExtent.columnName
    // Post aggregation left crs. We preserve the original name.
    val leftCRS2 = leftCRS.columnName
    // Post aggregation right extent. We create a new name.
    val rightExtent2 = id + "extent"
    // Post aggregation right crs. We create a new name.
    val rightCRS2 = id + "crs"

    // A representative tile from the left
    val leftTile = left.tileColumns.headOption.getOrElse(throw new IllegalArgumentException("Need at least one target tile on LHS"))


    val leftAggCols = left.columns.map(s => first(left(s), true) as s)
    val rightAggCtx = Seq(collect_list(rightExtent) as rightExtent2, collect_list(rightCRS) as rightCRS2)
    val rightAggTiles = right.tileColumns.map(c => collect_list(c) as c.columnName)
    val aggCols = leftAggCols ++ rightAggTiles ++ rightAggCtx
    val reprojCols = rightAggTiles.map(t => reproject_and_merge(col(leftExtent2), col(leftCRS2), col(t.columnName), col(rightExtent2), col(rightCRS2), rf_dimensions(leftTile)) as t.columnName)
    val finalCols = leftAggCols.map(c => col(c.columnName)) ++ reprojCols

    left
      .withColumn(id, monotonically_increasing_id())
      .join(right, joinExprs, joinType = "left")
      .groupBy(col(id))
      .agg(aggCols.head, aggCols.tail: _*)
      .select(finalCols: _*)
  }
}
