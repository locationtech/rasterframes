/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Astraea, Inc.
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
package org.locationtech.rasterframes

import java.nio.file.{Files, Path}

import com.typesafe.scalalogging.Logger
import geotrellis.raster.Tile
import geotrellis.raster.render.{ColorMap, ColorRamps}
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.vector.Extent
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.Geometry
import org.locationtech.rasterframes.ref.RasterRef
import org.locationtech.rasterframes.tiles.ProjectedRasterTile
import org.locationtech.rasterframes.util._
import org.scalactic.Tolerance
import org.scalatest._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import org.scalatest.matchers.{MatchResult, Matcher}
import org.slf4j.LoggerFactory

trait TestEnvironment extends AnyFunSpec with Matchers with Inspectors with Tolerance with RasterMatchers {
  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))


  lazy val scratchDir: Path = {
    val outputDir = Files.createTempDirectory("rf-scratch-")
    outputDir.toFile.deleteOnExit()
    outputDir
  }

  // allow 2 retries, should stabilize CI builds. https://spark.apache.org/docs/2.4.7/submitting-applications.html#master-urls
  def sparkMaster: String = "local[*, 2]"

  def additionalConf: SparkConf =
    new SparkConf(false)
      .set("spark.driver.port", "0")
      .set("spark.hostPort", "0")
      .set("spark.ui.enabled", "false")

  implicit val spark: SparkSession =
      SparkSession
        .builder
        .master(sparkMaster)
        .withKryoSerialization
        .config(additionalConf)
        .getOrCreate()
        .withRasterFrames

  implicit def sc: SparkContext = spark.sparkContext

  lazy val sql: String => DataFrame = spark.sql

  def isCI: Boolean = sys.env.get("CI").contains("true")

  /** This is here so we can test writing UDF generated/modified GeoTrellis types to ensure they are Parquet compliant. */
  def write(df: Dataset[_]): Boolean = {
    val sanitized = df.select(df.columns.map(c => col(c).as(toParquetFriendlyColumnName(c))): _*)
    val inRows = sanitized.count()
    val dest = Files.createTempFile("rf", ".parquet")
    logger.trace(s"Writing '${sanitized.columns.mkString(", ")}' to '$dest'...")
    sanitized.write.mode(SaveMode.Overwrite).parquet(dest.toString)
    val in = df.sparkSession.read.parquet(dest.toString)
    // NB: The `collect` ensures values get fully reified.
    val rows = in.collect()
    logger.trace(s" read back ${rows.length} row(s)")
    rows.length == inRows
  }

  def render(tile: Tile, tag: String): Unit = {
    val colors = ColorMap.fromQuantileBreaks(tile.histogram, ColorRamps.greyscale(128))
    val path = s"target/${getClass.getSimpleName}_$tag.png"
    logger.info(s"Writing '$path'")
    tile.color(colors).renderPng().write(path)
  }

  /**
   * Constructor for creating a DataFrame with a single row and no columns.
   * Useful for testing the invocation of data constructing UDFs.
   */
  def dfBlank(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(spark.sparkContext.makeRDD(Seq(Row())), StructType(Seq.empty))
  }

  /* Derived from GeoTrellis vector testkit */
  class GeometryMatcher[T <: Geometry](right: T, tolerance: Double) extends Matcher[T] {
    def doMatch(left: T): Boolean = left.equalsExact(right, tolerance)
    def apply(left: T) =
      MatchResult(
        doMatch(left),
        s"""$left did not match $right within tolerance $tolerance """,
        s"""$left matched $right within tolerance $tolerance"""
      )
  }

  def matchGeom(g: Geometry, tolerance: Double) = new GeometryMatcher(g, tolerance)

  def basicallySame(expected: Extent, computed: Extent): Unit = {
    val components = Seq(
      (expected.xmin, computed.xmin),
      (expected.ymin, computed.ymin),
      (expected.xmax, computed.xmax),
      (expected.ymax, computed.ymax)
    )
    forEvery(components)(c =>
      assert(c._1 === c._2 +- 0.000001)
    )
  }

  def checkDocs(name: String): Unit = {
    import spark.implicits._
    val docs = sql(s"DESCRIBE FUNCTION EXTENDED $name").as[String].collect().mkString("\n")
    docs should include(name)
    docs shouldNot include("not found")
    docs shouldNot include("null")
    docs shouldNot include("N/A")
  }

  implicit def prt2Enc: Encoder[(ProjectedRasterTile, ProjectedRasterTile)] = Encoders.tuple(ProjectedRasterTile.projectedRasterTileEncoder, ProjectedRasterTile.projectedRasterTileEncoder)
  implicit def prt3Enc: Encoder[(ProjectedRasterTile, ProjectedRasterTile, ProjectedRasterTile)] = Encoders.tuple(ProjectedRasterTile.projectedRasterTileEncoder, ProjectedRasterTile.projectedRasterTileEncoder, ProjectedRasterTile.projectedRasterTileEncoder)
  implicit def rr2Enc: Encoder[(RasterRef, RasterRef)] = Encoders.tuple(RasterRef.rasterRefEncoder, RasterRef.rasterRefEncoder)
  implicit def rr3Enc: Encoder[(RasterRef, RasterRef, RasterRef)] = Encoders.tuple(RasterRef.rasterRefEncoder, RasterRef.rasterRefEncoder, RasterRef.rasterRefEncoder)
}