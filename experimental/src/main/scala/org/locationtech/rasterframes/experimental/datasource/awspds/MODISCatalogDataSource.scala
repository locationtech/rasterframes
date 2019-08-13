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

package org.locationtech.rasterframes.experimental.datasource.awspds

import java.net.URI
import java.time.LocalDate
import java.time.temporal.ChronoUnit

import org.locationtech.rasterframes.util.withResource
import org.locationtech.rasterframes._
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.locationtech.rasterframes.experimental.datasource.ResourceCacheSupport


/**
 * DataSource over the catalog of AWS PDS for MODIS MCD43A4 Surface Reflectance data product
 * Param
 *
 * See https://docs.opendata.aws/modis-pds/readme.html for details
 *
 * @since 5/4/18
 */
class MODISCatalogDataSource extends DataSourceRegister with RelationProvider with LazyLogging  {
  override def shortName(): String = MODISCatalogDataSource.SHORT_NAME
  /**
     * Create a MODIS catalog data source.
     * @param sqlContext spark stuff
     * @param parameters optional parameters are:
     *                   `start`-start date for first scene files to fetch. default: "2013-01-01"
     *                   `end`-end date for last scene file to fetch. default: today's date - 7 days
     *                    `useBlacklist`-if false, ignore list of known missing scene files on AWS
     */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    require(parameters.get("path").isEmpty, "MODISCatalogDataSource doesn't support specifying a path. Please use `load()`.")

    sqlContext.withRasterFrames
    org.locationtech.rasterframes.experimental.datasource.register(sqlContext)

    val start = parameters.get("start").map(LocalDate.parse).getOrElse(LocalDate.of(2013, 1, 1))
    val end = parameters.get("end").map(LocalDate.parse).getOrElse(LocalDate.now().minusDays(7))
    val useBlacklist = parameters.get("useBlacklist").forall(_.toBoolean)

    val conf = sqlContext.sparkContext.hadoopConfiguration
    implicit val fs = FileSystem.get(conf)
    val path = MODISCatalogDataSource.sceneListFile(start, end, useBlacklist)
    MODISCatalogRelation(sqlContext, path)
  }
}

object MODISCatalogDataSource extends LazyLogging with ResourceCacheSupport {
  final val SHORT_NAME = "aws-pds-modis-catalog"
  final val MCD43A4_BASE = "https://modis-pds.s3.amazonaws.com/MCD43A4.006/"
  override def maxCacheFileAgeHours: Int = Int.MaxValue

  // List of missing days in PDS
  private val blacklist = Seq[String](
    "2018-02-27",
    "2018-02-28",
    "2018-03-01",
    "2018-03-02",
    "2018-03-03",
    "2018-03-04",
    "2018-03-05",
    "2018-03-06",
    "2018-03-07",
    "2018-03-08",
    "2018-03-09",
    "2018-03-10",
    "2018-03-11",
    "2018-03-12",
    "2018-03-13",
    "2018-03-14",
    "2018-05-16",
    "2018-05-17",
    "2018-05-18",
    "2018-05-19",
    "2018-05-20",
    "2018-05-21",
    "2018-06-01",
    "2018-06-04",
    "2018-07-29",
    "2018-08-03",
    "2018-08-04",
    "2018-08-05",
    "2018-10-01",
    "2018-10-02",
    "2018-10-03",
    "2018-10-22",
    "2018-10-23",
    "2018-11-12",
    "2018-12-19",
    "2018-12-20",
    "2018-12-21",
    "2018-12-22",
    "2018-12-23",
    "2018-12-24",
    "2019-03-18"
  )

  private def sceneFiles(start: LocalDate, end: LocalDate, useBlacklist: Boolean) = {
    val numDays = ChronoUnit.DAYS.between(start, end).toInt
    for {
      dayOffset <- 0 to numDays
      currDay = start.plusDays(dayOffset)
      if !useBlacklist || !blacklist.contains(currDay.toString)
    } yield URI.create(s"$MCD43A4_BASE${currDay}_scenes.txt")
  }

  private def sceneListFile(start: LocalDate, end: LocalDate, useBlacklist: Boolean)(implicit fs: FileSystem): HadoopPath = {
    logger.info(s"Using '$cacheDir' for scene file cache")
    val basename = new HadoopPath(s"$SHORT_NAME-$start-to-$end.csv")
    cachedFile(basename).getOrElse {
      val retval = cacheName(Right(basename))
      val inputs = sceneFiles(start, end, useBlacklist).par
        .flatMap(cachedURI(_))
        .toArray
      logger.debug(s"Concatinating scene files to '$retval':\n${inputs.mkString("\t" ,"\n\t", "\n")}")
      try {
        val dest = fs.create(retval)
        dest.hflush()
        dest.close()
        fs.concat(retval, inputs)
      }
      catch {
        case _ :UnsupportedOperationException ⇒
          // concat not supporty by RawLocalFileSystem
          withResource(fs.create(retval)) { out ⇒
            inputs.foreach { p ⇒
              withResource(fs.open(p)) { in ⇒
                IOUtils.copyBytes(in, out, 1 << 15)
              }
            }
          }
      }
      retval
    }
  }
}
