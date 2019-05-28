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

import geotrellis.proj4.{CRS, LatLng}
import org.locationtech.proj4j.CoordinateReferenceSystem
import org.locationtech.rasterframes.model.LazyCRS
import org.openjdk.jmh.annotations._

@BenchmarkMode(Array(Mode.AverageTime))
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
class CRSEncodingBench extends SparkEnv {

  var crs: CRS = _

  @Setup(Level.Invocation)
  def setupData(): Unit = {
    crs = LazyCRS("epsg:4326")
  }
  @Benchmark
  def resolveCRS(): CoordinateReferenceSystem = {
    crs.proj4jCrs
  }

  @Benchmark
  def selfEquals(): Boolean = {
    crs == crs
  }

  @Benchmark
  def logicalEquals(): Boolean = {
    crs == LatLng
  }
}
