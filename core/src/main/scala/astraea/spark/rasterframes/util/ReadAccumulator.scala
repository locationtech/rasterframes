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

package astraea.spark.rasterframes.util

import astraea.spark.rasterframes.ref.RasterSource
import astraea.spark.rasterframes.ref.RasterSource.ReadCallback
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator

/**
 * Support for keeping counts of read operations from RasterSource-s
 *
 * @since 9/3/18
 */
case class ReadAccumulator(reads: () ⇒ LongAccumulator, bytes: () ⇒ LongAccumulator) extends ReadCallback {
  override def readRange(source: RasterSource, start: Long, length: Int): Unit = {
    reads().add(1)
    bytes().add(length.toLong)
  }
  override def toString: String =
    s"${productPrefix}(reads=${reads().value}, bytes=${bytes().value})"
}

object ReadAccumulator extends LazyLogging {
  def apply(sc: SparkContext, prefix: String): ReadAccumulator = this.synchronized {
    val reads = sc.longAccumulator(prefix + ".reads")
    val bytes = sc.longAccumulator(prefix + ".bytes")
    new ReadAccumulator(() ⇒ reads, () ⇒ bytes)
  }
}