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
package org.locationtech.rasterframes.bench

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.rf.TileUDT
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.expressions.generators.ExplodeTiles
import org.openjdk.jmh.annotations._
/**
 *
 * @author sfitch
 * @since 10/4/17
 */
@BenchmarkMode(Array(Mode.AverageTime))
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class TileExplodeBench extends SparkEnv {

  //@Param(Array("uint8", "uint16ud255", "float32", "float64"))
  @Param(Array("uint16ud255"))
  var cellTypeName: String = _

  @Param(Array("256"))
  var tileSize: Int = _

  @Param(Array("2000"))
  var numTiles: Int = _

  @transient
  var tiles: Array[InternalRow] = _

  var exploder: ExplodeTiles = _

  @Setup(Level.Trial)
  def setupData(): Unit = {
    tiles = Array.fill(numTiles)(randomTile(tileSize, tileSize, cellTypeName))
        .map(t => InternalRow(TileUDT.tileSerializer.toInternalRow(t)))
    val expr = BoundReference(0, TileType, true)
    exploder = new ExplodeTiles(1.0, None, Seq(expr))
  }
  @Benchmark
  def tileExplode() = {
    for(t <- tiles)
      exploder.eval(t)
  }
}
