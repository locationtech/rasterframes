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

import org.locationtech.rasterframes.expressions.localops._
import org.locationtech.rasterframes._
import geotrellis.raster.Tile
import geotrellis.raster.mapalgebra.{local => gt}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.openjdk.jmh.annotations._
@BenchmarkMode(Array(Mode.AverageTime))
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class BinaryTileOpBench extends SparkEnv {
  import spark.implicits._

  @Param(Array("uint16ud255"))
  var cellTypeName: String = _

  @Param(Array("64"))
  var tileSize: Int = _

  @Param(Array("100"))
  var numTiles: Int = _

  @transient
  var tiles: DataFrame = _

  val localAddUDF = udf((left: Tile, right: Tile) => gt.Add(left, right))

  @Setup(Level.Trial)
  def setupData(): Unit = {
    tiles = Seq.fill(numTiles)((randomTile(tileSize, tileSize, cellTypeName), randomTile(tileSize, tileSize, cellTypeName)))
      .toDF("left", "right").repartition(10)
  }

  @Benchmark
  def viaExpression(): Array[Tile] = {
    tiles.select(Add($"left", $"right")).collect()
  }

  @Benchmark
  def viaUdf(): Array[Tile] = {
    tiles.select(localAddUDF($"left", $"right").as[Tile]).collect()
  }
}
