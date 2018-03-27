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

import geotrellis.raster.Tile
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.rf.InternalRowTile
import org.apache.spark.sql.gt.types.TileUDT
import org.openjdk.jmh.annotations._

@BenchmarkMode(Array(Mode.AverageTime))
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
/**
 * @author sfitch
 * @since 9/29/17
 */
class TileCellScanBench extends SparkEnv {

  @Param(Array("uint8", "int32", "float32", "float64"))
  var cellTypeName: String = _

  @Param(Array("512"))
  var tileSize: Int = _

  @transient
  var tileRow: InternalRow = _

  @Setup(Level.Trial)
  def setupData(): Unit = {
    tileRow = TileUDT.serialize(randomTile(tileSize, tileSize, cellTypeName))
  }

  @Benchmark
  def deserializeRead(): Double  = {
    val tile = TileUDT.deserialize(tileRow)
    val (cols, rows) = tile.dimensions
    tile.getDouble(cols - 1, rows - 1) +
      tile.getDouble(cols/2, rows/2) +
      tile.getDouble(0, 0)
  }

  @Benchmark
  def internalRowRead(): Double  = {
    val tile = new InternalRowTile(tileRow)
    val cols = tile.cols
    val rows = tile.rows
    tile.getDouble(cols - 1, rows - 1) +
      tile.getDouble(cols/2, rows/2) +
      tile.getDouble(0, 0)
  }
}

