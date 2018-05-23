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
import java.time.{Duration, Instant}

import astraea.spark.rasterframes.util._
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.{FileSystem, Path ⇒ HadoopPath}
import org.apache.hadoop.io.MD5Hash

import scala.util.Try
import scala.util.control.NonFatal

/**
 * Support for downloading scene files from AWS PDS and caching them.
 *
 * @since 5/4/18
 */
trait ResourceCacheSupport extends DownloadSupport { self: LazyLogging  ⇒
  def maxCacheFileAgeHours: Int = sys.props.get("rasterframes.resource.age.max")
    .flatMap(v ⇒ Try(v.toInt).toOption)
    .getOrElse(24)

  protected def expired(p: HadoopPath)(implicit fs: FileSystem): Boolean = {
    if(!fs.exists(p)) {
      logger.debug(s"'$p' does not yet exist")
      true
    }
    else {

      val time = fs.getFileStatus(p).getModificationTime
      val exp = Instant.ofEpochMilli(time).isAfter(Instant.now().plus(Duration.ofHours(maxCacheFileAgeHours)))
      if(exp) logger.debug(s"'$p' is expired with mod time of '$time'")
      exp
    }
  }

  protected def cacheDir(implicit fs: FileSystem): HadoopPath = {
    val home = fs.getHomeDirectory
    val cacheDir = new HadoopPath(home, ".rf_cache")
    if(!fs.exists(cacheDir)) fs.mkdirs(cacheDir)
    cacheDir
  }

  protected def cacheName(path: Either[URI, HadoopPath])(implicit fs: FileSystem): HadoopPath = {
    val (name, hash) = path match {
      case Left(uri) ⇒
        (uri.getPath, MD5Hash.digest(uri.toASCIIString))
      case Right(p) ⇒
        (p.toString, MD5Hash.digest(p.toString))
    }
    val basename = FilenameUtils.getBaseName(name)
    val extension = FilenameUtils.getExtension(name)
    val localFileName = s"$basename-$hash.$extension"
    new HadoopPath(cacheDir, localFileName)
  }

  protected def cachedURI(uri: URI)(implicit fs: FileSystem): Option[HadoopPath] = {
    val dest = cacheName(Left(uri))
    dest.when(f ⇒ !expired(f)).orElse {
      try {
        val bytes = downloadBytes(uri.toASCIIString)
        withResource(fs.create(dest))(_.write(bytes))
        Some(dest)
      }
      catch {
        case NonFatal(_) ⇒
          Try(fs.delete(dest, false))
          logger.warn(s"'$uri' not found")
          None
      }
    }
  }

  protected def cachedFile(fileName: HadoopPath)(implicit fs: FileSystem): Option[HadoopPath] = {
     val dest = cacheName(Right(fileName))
     dest.when(f ⇒ !expired(f))
   }
}
