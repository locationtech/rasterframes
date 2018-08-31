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

package astraea.spark.rasterframes.experimental.datasource.awspds

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileSystem, Path ⇒ HadoopPath}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.sources.BaseRelation
import astraea.spark.rasterframes.util._

/**
 * Mix-in for a data source that is cached as a parquet file.
 *
 * @since 8/24/18
 */
trait CachedDatasetRelation extends ResourceCacheSupport { self: BaseRelation with LazyLogging ⇒
  protected def cacheFile: HadoopPath
  protected def constructDataset: Dataset[Row]

  def buildScan(): RDD[Row] = {
    val conf = sqlContext.sparkContext.hadoopConfiguration
    implicit val fs: FileSystem = FileSystem.get(conf)
    val catalog = cacheFile.when(fs.exists)
      .map(p ⇒ {logger.debug("Reading " + p); p})
      .map(p ⇒ sqlContext.read.parquet(p.toString))
      .getOrElse {
        val scenes = constructDataset
        scenes.write.parquet(cacheFile.toString)
        scenes
      }

    catalog.rdd
  }
}
