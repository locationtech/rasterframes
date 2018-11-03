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
 */

package astraea.spark.rasterframes.bench

import java.util.concurrent.TimeUnit

import astraea.spark.rasterframes._
import org.apache.spark.sql._
import org.openjdk.jmh.annotations._

/**
 *
 * @author sfitch 
 * @since 10/4/17
 */
@BenchmarkMode(Array(Mode.AverageTime))
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class StatsComputeBench extends SparkEnv {
  import spark.implicits._

  @Param(Array("uint16ud255"))
  var cellTypeName: String = _

  @Param(Array("240"))
  var tileSize: Int = _

  @Param(Array("400"))
  var numTiles: Int = _

  @transient
  var tiles: DataFrame = _

  @Setup(Level.Trial)
  def setupData(): Unit = {
    tiles = Seq.fill(numTiles)(randomTile(tileSize, tileSize, cellTypeName))
      .toDF("tile").repartition(10)
  }

  @Benchmark
  def computeStats() = {
    tiles.select(aggStats($"tile")).collect()
  }

  @Benchmark
  def extractMean() = {
    tiles.select(aggStats($"tile").getField("mean")).map(_.getDouble(0)).collect()
  }

  @Benchmark
  def directMean() = {
    tiles.repartition(10).select(aggMean($"tile")).collect()
  }

//  @Benchmark
//  def computeCounts() = {
//    tiles.toDF("tile").select(dataCells($"tile") as "counts").agg(sum($"counts")).collect()
//  }


}
