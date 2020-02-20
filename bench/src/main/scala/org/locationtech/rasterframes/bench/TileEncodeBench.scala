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

import java.net.URI
import java.util.concurrent.TimeUnit

import org.locationtech.rasterframes.ref.RasterRef.RasterRefTile
import org.locationtech.rasterframes.ref.RasterRef
import geotrellis.raster.Tile
import geotrellis.vector.Extent
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.locationtech.rasterframes.ref.{RasterRef, RFRasterSource}
import org.openjdk.jmh.annotations._

@BenchmarkMode(Array(Mode.AverageTime))
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
class TileEncodeBench extends SparkEnv {

  val tileEncoder: ExpressionEncoder[Tile] = ExpressionEncoder()
  val boundEncoder = tileEncoder.resolveAndBind()

  @Param(Array("uint8", "int32", "float32", "float64", "rasterRef"))
  var cellTypeName: String = _

  @Param(Array("64", "512"))
  var tileSize: Int = _

  @transient
  var tile: Tile = _

  @Setup(Level.Trial)
  def setupData(): Unit = {
    cellTypeName match {
      case "rasterRef" ⇒
        val baseCOG = "https://s3-us-west-2.amazonaws.com/landsat-pds/c1/L8/149/039/LC08_L1TP_149039_20170411_20170415_01_T1/LC08_L1TP_149039_20170411_20170415_01_T1_B1.TIF"
        val extent = Extent(253785.0, 3235185.0, 485115.0, 3471015.0)
        tile = RasterRefTile(RasterRef(RFRasterSource(URI.create(baseCOG)), 0, Some(extent), None))
      case _ ⇒
        tile = randomTile(tileSize, tileSize, cellTypeName)
    }
  }

  @Benchmark
  def encode(): InternalRow = {
    tileEncoder.toRow(tile)
  }

  @Benchmark
  def roundTrip(): Tile = {
    val row = tileEncoder.toRow(tile)
    boundEncoder.fromRow(row)
  }
}

