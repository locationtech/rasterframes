/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea, Inc.
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

package astraea.spark.rasterframes.bench


import java.util.concurrent.TimeUnit

import astraea.spark.rasterframes
import astraea.spark.rasterframes._
import astraea.spark.rasterframes.expressions.RasterSourceToTiles
import astraea.spark.rasterframes.ref.RasterSource
import astraea.spark.rasterframes.ref.RasterSource.ReadCallback
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._
import org.openjdk.jmh.annotations._
/**
 *
 *
 * @since 11/1/18
 */
@BenchmarkMode(Array(Mode.AverageTime))
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class RasterRefBench  extends SparkEnv with LazyLogging {
  import spark.implicits._

  var expandedDF: DataFrame = _
  var singleDF: DataFrame = _

  @Setup(Level.Trial)
  def setupData(): Unit = {
    val watcher = new ReadCallback {
      var count: Long = 0
      var calls: Int = 0
      override def readRange(source: RasterSource, start: Long, length: Int): Unit = {
        calls += 1
        count += length
        logger.debug("%4d -- %,d bytes".format(calls, count))
      }
    }

    val r1 = RasterSource(remoteCOGSingleband1, Some(watcher))
    val r2 = RasterSource(remoteCOGSingleband2, Some(watcher))
    singleDF = Seq((r1, r2)).toDF("B1", "B2")
      .select(RasterSourceToTiles(false, $"B1", $"B2"))

    expandedDF = Seq((r1, r2)).toDF("B1", "B2")
      .select(RasterSourceToTiles(true, $"B1", $"B2"))
  }

  @Benchmark
  def computeDifferenceExpanded() = {
    expandedDF
      .select(normalizedDifference($"B1", $"B2"))
      .cache()
      .count()
  }

  @Benchmark
  def computeDifferenceSingle() = {
    singleDF
      .select(normalizedDifference($"B1", $"B2"))
      .cache()
      .count()
  }

  @Benchmark
  def computeStatsSingle() = {
    singleDF.select(aggStats($"B1")).collect()
  }

  @Benchmark
  def computeStatsExpanded() = {
    expandedDF.select(aggStats($"B1")).collect()
  }

  @Benchmark
  def computeDifferenceStats() = {
    singleDF.select(aggStats(normalizedDifference($"B1", $"B2"))).collect()
  }

}

object RasterRefBench {

//  import org.openjdk.jmh.runner.RunnerException
//  import org.openjdk.jmh.runner.options.OptionsBuilder
//
//  @throws[RunnerException]
  def main(args: Array[String]): Unit = {

  val thing = new RasterRefBench()
  thing.setupData()
  rasterframes.util.time("compute stats expanded") {
    thing.computeStatsSingle()
  }

  rasterframes.util.time("compute stats single") {
    thing.computeStatsExpanded()
  }

  //    val opt = new OptionsBuilder()
//      .include(classOf[RasterRefBench].getSimpleName)
//      .threads(4)
//      .forks(5)
//      .build()
//
//    new Runner(opt).run()
  }
}
