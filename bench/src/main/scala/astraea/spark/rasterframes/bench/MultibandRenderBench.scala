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

package astraea.spark.rasterframes.bench
import java.util.concurrent.TimeUnit

import astraea.spark.rasterframes.util.MultibandRender.Landsat8NaturalColor
import geotrellis.raster._
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import org.apache.commons.io.IOUtils
import org.openjdk.jmh.annotations._

@BenchmarkMode(Array(Mode.AverageTime))
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 10)
class MultibandRenderBench {
  var mbt: MultibandTile = _

  @Setup(Level.Trial)
  def setupData(): Unit = {
    mbt = MultibandGeoTiff(IOUtils.toByteArray(
      getClass.getResourceAsStream("/LC08_RGB_Norfolk_COG.tiff")
    )).tile.toArrayTile()
  }

  @Benchmark
  def render() = {
    Landsat8NaturalColor.render(mbt)
  }
}
