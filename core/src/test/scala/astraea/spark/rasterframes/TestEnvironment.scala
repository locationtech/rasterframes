/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright (c) 2017. Astraea, Inc.
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
 */
package astraea.spark.rasterframes

import java.nio.file.{Files, Paths}

import astraea.spark.rasterframes.encoders.StandardEncoders.PrimitiveEncoders.stringEnc
import astraea.spark.rasterframes.ref.RasterSource
import astraea.spark.rasterframes.ref.RasterSource.ReadCallback
import astraea.spark.rasterframes.util.toParquetFriendlyColumnName
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.Geometry
import geotrellis.spark.testkit.{TestEnvironment => GeoTrellisTestEnvironment}
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.scalactic.Tolerance
import org.scalatest._
import org.scalatest.matchers.{MatchResult, Matcher}

trait TestEnvironment extends FunSpec with GeoTrellisTestEnvironment
  with Matchers with Inspectors with Tolerance with LazyLogging {

  override def sparkMaster: String = "local[*]"

  override implicit def sc: SparkContext = { _sc.setLogLevel("ERROR"); _sc }
  //p.setProperty(“spark.driver.allowMultipleContexts”, “true”)

  lazy val sqlContext: SQLContext = {
    val session = SparkSession.builder.config(_sc.getConf).getOrCreate()
    astraea.spark.rasterframes.WithSQLContextMethods(session.sqlContext).withRasterFrames
  }

  lazy val sql: String ⇒ DataFrame = sqlContext.sql
  implicit lazy val spark: SparkSession = sqlContext.sparkSession

  def isCI: Boolean = sys.env.get("CI").contains("true")

  /** This is here so we can test writing UDF generated/modified GeoTrellis types to ensure they are Parquet compliant. */
  def write(df: Dataset[_]): Boolean = {
    val sanitized = df.select(df.columns.map(c ⇒ col(c).as(toParquetFriendlyColumnName(c))): _*)
    val inRows = sanitized.count()
    val dest = Files.createTempFile(Paths.get(outputLocalPath), "rf", ".parquet")
    logger.trace(s"Writing '${sanitized.columns.mkString(", ")}' to '$dest'...")
    sanitized.write.mode(SaveMode.Overwrite).parquet(dest.toString)
    val rows = df.sparkSession.read.parquet(dest.toString).count()
    logger.trace(s" read back $rows row(s)")
    rows == inRows
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

  def checkDocs(name: String): Unit = {
    val docs = sql(s"DESCRIBE FUNCTION EXTENDED $name").as[String].collect().mkString("\n")
    docs should include(name)
    docs shouldNot include("not found")
    docs shouldNot include("null")
    docs shouldNot include("N/A")
  }
}

object TestEnvironment {
  case class ReadMonitor(ignoreHeader: Boolean = true) extends ReadCallback with LazyLogging {
    var reads: Int = 0
    var total: Long = 0
    override def readRange(source: RasterSource, start: Long, length: Int): Unit = {
      logger.trace(s"Reading $length at $start from $source")
      // Ignore header reads
      if(!ignoreHeader || start > 0) reads += 1
      total += length
    }

    override def toString: String = s"$productPrefix(reads=$reads, total=$total)"
  }
}