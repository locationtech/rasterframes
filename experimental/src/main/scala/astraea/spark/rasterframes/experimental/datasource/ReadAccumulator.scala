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

package astraea.spark.rasterframes.experimental.datasource

import astraea.spark.rasterframes.ref.RasterSource
import astraea.spark.rasterframes.ref.RasterSource.ReadCallback
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable

/**
 * Support for keeping counts of read operations from RasterSource-s
 *
 * @since 9/3/18
 */
case class ReadAccumulator(reads: () ⇒ LongAccumulator, bytes: () ⇒ LongAccumulator) extends ReadCallback {
  override def readRange(source: RasterSource, start: Long, length: Int): Unit = {
    reads().add(1)
    bytes().add(length)
  }
  override def toString: String =
    s"${productPrefix}(reads=${reads().value}, bytes=${bytes().value})"
}

object ReadAccumulator extends LazyLogging {
  private val reads: mutable.Map[String, LongAccumulator] = mutable.Map.empty
  private val bytes: mutable.Map[String, LongAccumulator] = mutable.Map.empty

  def apply(sc: SparkContext, prefix: String): ReadAccumulator = this.synchronized {
    // TODO: Not sure how inititalize these in the proper scope... this is not it.
    reads.getOrElseUpdate(prefix, sc.longAccumulator(prefix + ".reads"))//.reset()
    bytes.getOrElseUpdate(prefix, sc.longAccumulator(prefix + ".bytes"))//.reset()
    new ReadAccumulator(() ⇒ reads(prefix), () ⇒ bytes(prefix))
  }

  def log(): Unit = this.synchronized {
    val keys = reads.keySet.intersect(bytes.keySet)
    keys.foreach { key ⇒
      val r = reads(key).value
      val b = bytes(key).value
      logger.info(s"readCount=$r, totalBytes=$b")
    }
  }
}