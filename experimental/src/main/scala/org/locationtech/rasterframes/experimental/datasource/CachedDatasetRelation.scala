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

package org.locationtech.rasterframes.experimental.datasource

import better.files.File
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.{Dataset, Row, SaveMode}
import org.locationtech.rasterframes.util._

/**
 * Mix-in for a data source that is cached as a parquet file.
 *
 * @since 8/24/18
 */
trait CachedDatasetRelation extends ResourceCacheSupport { self: BaseRelation =>
  protected def defaultNumPartitions: Int =
    sqlContext.sparkSession.sessionState.conf.numShufflePartitions
  protected def cacheFile: File
  protected def constructDataset: Dataset[Row]

  def buildScan(): RDD[Row] = {
    val conf = sqlContext.sparkContext.hadoopConfiguration
    val catalog = cacheFile.when(p => p.exists && !expired(p))
      .map(p => {logger.debug("Reading " + p); p})
      .map(p => sqlContext.read.parquet(p.toString))
      .getOrElse {
        val scenes = constructDataset
        scenes.write.mode(SaveMode.Overwrite).parquet(cacheFile.toString)
        scenes
      }

    catalog.rdd
  }
}
