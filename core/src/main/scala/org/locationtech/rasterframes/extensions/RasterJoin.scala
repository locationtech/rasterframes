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
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataType
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.expressions.accessors.ExtractTile
import org.locationtech.rasterframes.expressions.{DynamicExtractors, SpatialRelation}
import org.locationtech.rasterframes.functions.reproject_and_merge
import org.locationtech.rasterframes.util._

import scala.util.Random

object RasterJoin {

  /** Perform a raster join on dataframes that each have proj_raster columns, or crs and extent explicitly included. */
  def apply(left: DataFrame, right: DataFrame): DataFrame = {
    def usePRT(d: DataFrame) =
      d.projRasterColumns.headOption
        .map(p => (rf_crs(p),  rf_extent(p)))
        .orElse(Some(col("crs"), col("extent")))
        .map { case (crs, extent) =>
          val d2 = d.withColumn("crs", crs).withColumn("extent", extent)
          (d2, d2("crs"), d2("extent"))
        }
        .get

    val (ldf, lcrs, lextent) = usePRT(left)
    val (rdf, rcrs, rextent) = usePRT(right)

    apply(ldf, rdf, lextent, lcrs, rextent, rcrs)
  }

  def apply(left: DataFrame, right: DataFrame, leftExtent: Column, leftCRS: Column, rightExtent: Column, rightCRS: Column): DataFrame = {
    val leftGeom = st_geometry(leftExtent)
    val rightGeomReproj = st_reproject(st_geometry(rightExtent), rightCRS, leftCRS)
    val joinExpr = new Column(SpatialRelation.Intersects(leftGeom.expr, rightGeomReproj.expr))
    apply(left, right, joinExpr, leftExtent, leftCRS, rightExtent, rightCRS)
  }

  private def checkType[T](col: Column, description: String, extractor: PartialFunction[DataType, Any => T]): Unit = {
    require(extractor.isDefinedAt(col.expr.dataType), s"Expected column ${col} to be of type $description, but was ${col.expr.dataType}.")
  }

  def apply(left: DataFrame, right: DataFrame, joinExprs: Column, leftExtent: Column, leftCRS: Column, rightExtent: Column, rightCRS: Column): DataFrame = {
    // Convert resolved column into a symbolic one.
    def unresolved(c: Column): Column = col(c.columnName)

    checkType(leftExtent, "Extent", DynamicExtractors.extentExtractor)
    checkType(leftCRS, "CRS", DynamicExtractors.crsExtractor)
    checkType(rightExtent, "Extent", DynamicExtractors.extentExtractor)
    checkType(rightCRS, "CRS", DynamicExtractors.crsExtractor)

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


    // Gathering up various expressions we'll use to construct the result.
    // After joining We will be doing a groupBy the LHS. We have to define the aggregations to perform after the groupBy.
    // On the LHS we just want the first thing (subsequent ones should be identical.
    val leftAggCols = left.columns.map(s => first(left(s), true) as s)
    // On the RHS we collect result as a list.
    val rightAggCtx = Seq(collect_list(rightExtent) as rightExtent2, collect_list(rf_crs(rightCRS)) as rightCRS2)
    val rightAggTiles = right.tileColumns.map(c => collect_list(ExtractTile(c)) as c.columnName)
    val rightAggOther = right.notTileColumns
      .filter(n => n.columnName != rightExtent.columnName && n.columnName != rightCRS.columnName)
      .map(c => collect_list(c) as (c.columnName + "_agg"))
    val aggCols = leftAggCols ++ rightAggTiles ++ rightAggCtx ++ rightAggOther

    // After the aggregation we take all the tiles we've collected and resample + merge
    // into LHS extent/CRS.
    // Use a representative tile from the left for the tile dimensions.
    // Assumes all LHS tiles in a row are of the same size.
    val destDims = rf_dimensions(coalesce(left.tileColumns.map(unresolved): _*))

    val reprojCols = rightAggTiles.map(t => {
      reproject_and_merge(
        col(leftExtent2), col(leftCRS2), col(t.columnName), col(rightExtent2), col(rightCRS2), destDims
      ) as t.columnName
    })

    val finalCols = leftAggCols.map(unresolved) ++ reprojCols ++ rightAggOther.map(unresolved)

    // Here's the meat:
    left
      // 1. Add a unique ID to each LHS row for subequent grouping.
      .withColumn(id, monotonically_increasing_id())
      // 2. Perform the left-outer join
      .join(right, joinExprs, joinType = "left")
      // 3. Group by the unique ID, reestablishing the LHS count
      .groupBy(col(id))
      // 4. Apply aggregation to left and right columns:
      //    a. LHS just take the first entity
      //    b. RHS collect all results in a list
      .agg(aggCols.head, aggCols.tail: _*)
      // 5. Perform merge on RHC tile column collections, pass everything else through.
      .select(finalCols: _*)
  }
}
