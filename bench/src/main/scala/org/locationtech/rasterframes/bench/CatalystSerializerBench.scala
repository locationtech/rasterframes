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

import org.locationtech.rasterframes.encoders.CatalystSerializer
import geotrellis.proj4.{CRS, LatLng, Sinusoidal}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.locationtech.rasterframes.encoders.{CatalystSerializer, StandardEncoders}
import org.openjdk.jmh.annotations._

@BenchmarkMode(Array(Mode.AverageTime))
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
class CatalystSerializerBench extends SparkEnv {

  val serde = CatalystSerializer[CRS]

  val epsg: CRS = LatLng
  val epsgEnc: Row = serde.toRow(epsg)
  val proj4: CRS = Sinusoidal
  val proj4Enc: Row = serde.toRow(proj4)

  var crsEnc: ExpressionEncoder[CRS] = _

  @Setup(Level.Trial)
  def setupData(): Unit = {
    crsEnc = StandardEncoders.crsEncoder.resolveAndBind()
  }

  @Benchmark
  def encodeEpsg(): Row = {
    serde.toRow(epsg)
  }

  @Benchmark
  def encodeProj4(): Row = {
    serde.toRow(proj4)
  }

  @Benchmark
  def decodeEpsg(): CRS = {
    serde.fromRow(epsgEnc)
  }

  @Benchmark
  def decodeProj4(): CRS = {
    serde.fromRow(proj4Enc)
  }

  @Benchmark
  def exprEncodeEpsg(): InternalRow = {
    crsEnc.toRow(epsg)
  }

  @Benchmark
  def exprEncodeProj4(): InternalRow = {
    crsEnc.toRow(proj4)
  }

//  @Benchmark
//  def exprDecodeEpsg(): CRS = {
//
//  }
//
//  @Benchmark
//  def exprDecodeProj4(): CRS = {
//
//  }

}
