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

import astraea.spark.rasterframes._
import geotrellis.raster.ByteConstantNoDataCellType
import org.apache.spark.sql._
import org.openjdk.jmh.annotations._

@BenchmarkMode(Array(Mode.AverageTime))
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class TileAssembleBench extends SparkEnv {
  import spark.implicits._

  val cellType = ByteConstantNoDataCellType

  @Param(Array("256", "512"))
  var tileSize: Int = _

  @Param(Array("100"))
  var numTiles: Int = _

  var cells1: DataFrame = _
  var cells2: DataFrame = _

  val assembler = assembleTile(
    $"column_index", $"row_index", $"tile",
    tileSize, tileSize, cellType
  )

  @Setup(Level.Trial)
  def setupData(): Unit = {
    cells1 = Seq.fill(numTiles)(randomTile(tileSize, tileSize, cellType.name)).zipWithIndex
      .toDF("tile", "id")
      .select($"id", explodeTiles($"tile"))
      .repartition(4, $"id")
      .cache()

    cells1.count()

    // basically shuffle the rows
    cells2 = cells1
      .repartition(4,$"tile")
      .cache()

    cells2.count()
  }

  @Benchmark
  def assemble() = {
    cells1
      .groupBy($"id")
      .agg(assembler)
      .count()
  }

  @Benchmark
  def assembleShuffled() = {
    cells2
      .groupBy($"id")
      .agg(assembler)
      .count()
  }
}
