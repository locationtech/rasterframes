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

import geotrellis.spark.testkit.{TestEnvironment ⇒ GeoTrellisTestEnvironment}
import geotrellis.util.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SaveMode}
import org.scalactic.Tolerance
import org.scalatest._

trait TestEnvironment extends FunSpec with GeoTrellisTestEnvironment
  with Matchers with Inspectors with Tolerance with LazyLogging {

  override implicit def sc: SparkContext = { _sc.setLogLevel("ERROR"); _sc }

  lazy val sqlContext = {
    val ctx = SQLContext.getOrCreate(_sc).withRasterFrames
    ctx
  }

  lazy val sql: (String) ⇒ DataFrame = sqlContext.sql
  implicit lazy val spark = sqlContext.sparkSession

  def isCI: Boolean = sys.env.get("CI").contains("true")

  /** This is here so we can test writing UDF generated/modified GeoTrellis types to ensure they are Parquet compliant. */
  def write(df: Dataset[_]): Unit = {
    val sanitized = df.select(df.columns.map(c ⇒ col(c).as(c.replaceAll("[ ,;{}()\n\t=]", "_"))): _*)
    val dest = Files.createTempFile(Paths.get(outputLocalPath), "GTSQL", ".parquet")
    logger.debug(s"Writing '${sanitized.columns.mkString(", ")}' to '$dest'...")
    sanitized.write.mode(SaveMode.Overwrite).parquet(dest.toString)
    val rows = df.sparkSession.read.parquet(dest.toString).count()
    logger.debug(s" it has $rows row(s)")
  }

}
