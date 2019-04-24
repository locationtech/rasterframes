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

package org.locationtech.rasterframes.bench

import java.util.concurrent.TimeUnit

import org.locationtech.rasterframes._
import org.locationtech.rasterframes.stats.CellHistogram
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

//  @Benchmark
//  def computeStats(): Array[CellStatistics] = {
//    tiles.select(agg_stats($"tile")).collect()
//  }

  @Benchmark
  def computeHistogram(): Array[CellHistogram] = {
    tiles.select(agg_approx_histogram($"tile")).collect()
  }

//  @Benchmark
//  def extractMean(): Array[Double] = {
//    tiles.select(agg_stats($"tile").getField("mean")).map(_.getDouble(0)).collect()
//  }
//
//  @Benchmark
//  def directMean(): Array[Double] = {
//    tiles.repartition(10).select(agg_mean($"tile")).collect()
//  }

//  @Benchmark
//  def computeCounts() = {
//    tiles.toDF("tile").select(data_cells($"tile") as "counts").agg(sum($"counts")).collect()
//  }
}
