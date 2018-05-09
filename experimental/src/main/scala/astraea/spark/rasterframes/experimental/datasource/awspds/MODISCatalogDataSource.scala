/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea. Inc.
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
 *
 */

package astraea.spark.rasterframes.experimental.datasource.awspds

import java.net.URI
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.time.LocalDate
import java.time.temporal.ChronoUnit

import com.typesafe.scalalogging.{LazyLogging, StrictLogging}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import scalaz.stream.nio.file

/**
 * DataSource over the catalog of AWS PDS for MODIS MCD43A4 Surface Reflectance data product
 * Param
 *
 * See https://docs.opendata.aws/modis-pds/readme.html for details
 *
 * @since 5/4/18
 */
class MODISCatalogDataSource extends DataSourceRegister with RelationProvider {
  override def shortName(): String = MODISCatalogDataSource.NAME
  /**
     * Create a MODIS catalog data source.
     * @param sqlContext spark stuff
     * @param parameters optional parameters are:
     *                   `path`-path to aggregate scene file. Only specify if you don't want one constructed
     *                   `start`-start date for first scene files to fetch. default: "2013-01-01"
     *                   `end`-end date for last scene file to fetch. default: today's date - 7 days
     *                    `useBlacklist`-if false, ignore list of known missing scene files on AWS
     */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val start = parameters.get("start").map(LocalDate.parse).getOrElse(LocalDate.of(2013, 1, 1))
    val end = parameters.get("end").map(LocalDate.parse).getOrElse(LocalDate.now().minusDays(7))
    val useBlacklist = parameters.get("useBlacklist").forall(_.toBoolean)
    val path = parameters.getOrElse("path", "file://" + MODISCatalogDataSource.sceneListFile(start, end, useBlacklist).toAbsolutePath.toString)
    MODISCatalogRelation(sqlContext, path)
  }
}

object MODISCatalogDataSource extends LazyLogging with ResourceCacheSupport {
  val NAME = "modis-catalog"
  val MCD43A4_BASE = "https://modis-pds.s3.amazonaws.com/MCD43A4.006/"
  override def maxCacheFileAgeHours: Int = Int.MaxValue

  // As of 5/6/2018, these days are missing from AWS.
  private val blacklist = Seq(
    "2018-03-07",
    "2018-03-08",
    "2018-03-09",
    "2018-03-10",
    "2018-03-11",
    "2018-03-12",
    "2018-03-13",
    "2018-03-14",
    "2018-03-15",
    "2018-02-27",
    "2018-02-28",
    "2018-03-01",
    "2018-03-02",
    "2018-03-03",
    "2018-03-04",
    "2018-04-27",
    "2018-03-05",
    "2018-04-28",
    "2018-03-06",
    "2018-04-29",
    "2018-04-30",
    "2018-05-01",
    "2018-05-02",
    "2018-05-03",
    "2018-05-04",
    "2018-05-05",
    "2018-05-06"
  )

  private def sceneFiles(start: LocalDate, end: LocalDate, useBlacklist: Boolean) = {
    val numDays = ChronoUnit.DAYS.between(start, end).toInt
    for {
      dayOffset <- 0 to numDays
      currDay = start.plusDays(dayOffset)
      if !useBlacklist || !blacklist.contains(currDay.toString)
    } yield URI.create(s"$MCD43A4_BASE${currDay}_scenes.txt")
  }

  private def sceneListFile(start: LocalDate, end: LocalDate, useBlacklist: Boolean) = {
    logger.info(s"Using '$cacheDir' for scene file cache")
    val basename = Paths.get(s"$NAME-$start-to-$end.csv")
    cachedFile(basename).getOrElse {
      val retval = cacheName(Right(basename))
      Files.write(retval, "date,download_url,gid\n".getBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
      sceneFiles(start, end, useBlacklist).par
        .flatMap(cachedURI)
        .toArray
        .foreach(p ⇒ {
          val content = Files.readAllLines(p)
          if(!content.isEmpty) {
            // Drop header
            content.remove(0)
            Files.write(retval, content, StandardOpenOption.APPEND, StandardOpenOption.DSYNC)
          }
        })

      retval
    }
  }
}
