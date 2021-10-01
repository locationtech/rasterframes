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
import org.apache.commons.io.{FilenameUtils, IOUtils}
import org.apache.hadoop.io.MD5Hash
import org.locationtech.rasterframes.util._

import java.net.URI
import java.time.{Duration, Instant}
import scala.util.Try
import scala.util.control.NonFatal

/**
 * Support for downloading scene files from AWS PDS and caching them.
 *
 * @since 5/4/18
 */
trait ResourceCacheSupport {
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  def maxCacheFileAgeHours: Int = sys.props.get("rasterframes.resource.age.max")
    .flatMap(v => Try(v.toInt).toOption)
    .getOrElse(24)

  protected def expired(p: File): Boolean = {
    if(!p.exists) {
      logger.debug(s"'$p' does not yet exist")
      true
    }
    else {
      val time = p.lastModifiedTime
      val exp = time.plus(Duration.ofHours(maxCacheFileAgeHours)).isBefore(Instant.now())
      if(exp) logger.debug(s"'$p' is expired with mod time of '$time'")
      exp
    }
  }

  protected def cacheDir: File = {
    val home = File.home
    val cacheDir = home / ".rf_cache"
    cacheDir.createDirectoryIfNotExists()
    cacheDir
  }

  protected def cacheName(path: Either[URI, File]): File = {
    val (name, hash) = path match {
      case Left(uri) =>
        (uri.getPath, MD5Hash.digest(uri.toASCIIString))
      case Right(p) =>
        (p.toString, MD5Hash.digest(p.toString))
    }
    val basename = FilenameUtils.getBaseName(name)
    val extension = FilenameUtils.getExtension(name)
    val localFileName = s"$basename-$hash.$extension"
    cacheDir / localFileName
  }

  protected def cachedURI(uri: URI): Option[File] = {
    val dest = cacheName(Left(uri))
    dest.when(f => !expired(f)).orElse {
      try {
        val bytes = IOUtils.toByteArray(uri)
        dest.createFile().writeByteArray(bytes)
        Some(dest)
      }
      catch {
        case NonFatal(_) =>
          dest.delete(true)
          logger.debug(s"'$uri' not found")
          None
      }
    }
  }

  protected def cachedFile(fileName: File): Option[File] = {
     val dest = cacheName(Right(fileName))
     dest.when(f => !expired(f))
   }
}
