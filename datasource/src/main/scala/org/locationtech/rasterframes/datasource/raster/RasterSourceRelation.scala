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

import geotrellis.raster.Dimensions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.locationtech.rasterframes.datasource.raster.RasterSourceDataSource.RasterSourceCatalogRef
import org.locationtech.rasterframes.expressions.accessors.{GetCRS, GetExtent}
import org.locationtech.rasterframes.expressions.generators.{RasterSourceToRasterRefs, RasterSourceToTiles}
import org.locationtech.rasterframes.expressions.generators.RasterSourceToRasterRefs.bandNames
import org.locationtech.rasterframes.expressions.transformers.{RasterRefToTile, URIToRasterSource, XZ2Indexer}
import org.locationtech.rasterframes.tiles.ProjectedRasterTile

/**
 * Constructs a Spark Relation over one or more RasterSource paths.
 * @param sqlContext Query context
 * @param catalogTable Specification of raster path sources
 * @param bandIndexes band indexes to fetch
 * @param subtileDims how big to tile/subdivide rasters info
 * @param lazyTiles if true, creates a lazy representation of tile instead of fetching contents.
 * @param spatialIndexPartitions Number of spatial index-based partitions to create.
 *                               If Option value > 0, that number of partitions are created after adding a spatial index.
 *                               If Option value <= 0, uses the value of `numShufflePartitions` in SparkContext.
 *                               If None, no spatial index is added and hash partitioning is used.
 */
case class RasterSourceRelation(
  sqlContext: SQLContext,
  catalogTable: RasterSourceCatalogRef,
  bandIndexes: Seq[Int],
  subtileDims: Option[Dimensions[Int]],
  lazyTiles: Boolean,
  spatialIndexPartitions: Option[Int]
) extends BaseRelation with TableScan {

  lazy val inputColNames = catalogTable.bandColumnNames

  def pathColNames = inputColNames
    .map(_ + "_path")

  def srcColNames = inputColNames
    .map(_ + "_src")

  def refColNames = srcColNames
    .flatMap(bandNames(_, bandIndexes))
    .map(_ + "_ref")

  def tileColNames = inputColNames
    .flatMap(bandNames(_, bandIndexes))

  lazy val extraCols: Seq[StructField] = {
    val catalog = sqlContext.table(catalogTable.tableName)
    catalog.schema.fields.filter(f => !catalogTable.bandColumnNames.contains(f.name))
  }

  lazy val indexCols: Seq[StructField] =
    if (spatialIndexPartitions.isDefined) Seq(StructField("spatial_index", LongType, false)) else Seq.empty

  protected def defaultNumPartitions: Int =
    sqlContext.sparkSession.sessionState.conf.numShufflePartitions

  override def schema: StructType = {
    val tileSchema = ProjectedRasterTile.projectedRasterTileEncoder.schema
    val paths = for {
      pathCol <- pathColNames
    } yield StructField(pathCol, StringType, false)
    val tiles = for {
      tileColName <- tileColNames
    } yield StructField(tileColName, tileSchema, true)

    StructType(paths ++ tiles ++ extraCols ++ indexCols)
  }

  override def buildScan(): RDD[Row] = {
    import sqlContext.implicits._
    val numParts = spatialIndexPartitions.filter(_ > 0).getOrElse(defaultNumPartitions)
    // The general transformation is:
    // input -> path -> src -> ref -> tile
    // Each step is broken down for readability
    val inputs: DataFrame = sqlContext.table(catalogTable.tableName)
      .repartition(numParts)

    // Basically renames the input columns to have the '_path' suffix
    val pathsAliasing = for {
      (input, path) <- inputColNames.zip(pathColNames)
    } yield col(input).as(path)

    // Wraps paths in a RasterSource
    val srcs = for {
      (pathColName, srcColName) <- pathColNames.zip(srcColNames)
    } yield URIToRasterSource(col(pathColName)) as srcColName

    // Add path columns
    val withPaths = inputs
      .select($"*" +: pathsAliasing: _*)

    // Path columns have to be manually pulled along through each step. Resolve columns once
    // and reused with each select.
    val paths = pathColNames.map(withPaths.apply)

    // Input columns along for the ride.
    val extras = extraCols.map(f => inputs(f.name))

    val df = if (lazyTiles) {
      // Expand RasterSource into multiple columns per band, and multiple rows per tile
      // There's some unintentional fragility here in that the structure of the expression
      // is expected to line up with our column structure here.
      val refs = RasterSourceToRasterRefs(subtileDims, bandIndexes, srcs: _*) as refColNames

      // RasterSourceToRasterRef is a generator, which means you have to do the Tile conversion
      // in a separate select statement (Query planner doesn't know how many columns ahead of time).
      val refsToTiles = for {
        (refColName, tileColName) <- refColNames.zip(tileColNames)
      } yield RasterRefToTile(col(refColName)) as tileColName

      withPaths
        .select(extras ++ paths :+ refs: _*)
        .select(paths ++ refsToTiles ++ extras: _*)
    } else {
      val tiles = RasterSourceToTiles(subtileDims, bandIndexes, srcs: _*) as tileColNames
      withPaths.select((paths :+ tiles) ++ extras: _*)
    }

    if (spatialIndexPartitions.isDefined) {
      val sample = col(tileColNames.head)
      val indexed = df
        .withColumn("spatial_index", XZ2Indexer(GetExtent(sample), GetCRS(sample)))
        .repartitionByRange(numParts,$"spatial_index")
      indexed.rdd
    }
    else df.repartition(numParts).rdd
  }
}
