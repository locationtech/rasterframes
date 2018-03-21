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

import geotrellis.raster.histogram.{Histogram, StreamingHistogram}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.gt.types.HistogramUDT
import org.openjdk.jmh.annotations._

@BenchmarkMode(Array(Mode.AverageTime))
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
/**
 * @author sfitch
 * @since 9/29/17
 */
class HistogramEncodeBench extends SparkEnv {

  val encoder: ExpressionEncoder[Histogram[Double]] = ExpressionEncoder()
  val boundEncoder = encoder.resolveAndBind()

  @Param(Array("float64"))
  var cellTypeName: String = _

  @Param(Array("128"))
  var tileSize: Int = _

  @Param(Array("400"))
  var numTiles: Int = _

  @transient
  var histogram: Histogram[Double] = _

  @Setup(Level.Trial)
  def setupData(): Unit = {
    // Creates what should hopefully be a representative structure
    val tiles = Seq.fill(numTiles)(randomTile(tileSize, tileSize, cellTypeName))
    histogram = tiles.foldLeft(StreamingHistogram(): Histogram[Double])(
      (hist, tile) ⇒ hist.merge(StreamingHistogram.fromTile(tile))
    )
  }
  @Benchmark
  def serialize(): Any = {
    HistogramUDT.serialize(histogram)
  }

  @Benchmark
  def encode(): InternalRow = {
    boundEncoder.toRow(histogram)
  }

//  @Benchmark
//  def roundTrip(): Tile = {
//  }
}

