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

package astraea.spark.rasterframes.ref

import java.net.{HttpURLConnection, URI}
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset, ZonedDateTime}

import astraea.spark.rasterframes
import astraea.spark.rasterframes.util.withResource
import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.Tags
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff.reader.GeoTiffReader.GeoTiffInfo
import geotrellis.raster.io.geotiff.tags.TiffTags
import geotrellis.spark.io.http.util.HttpRangeReader
import geotrellis.vector.Extent
import org.locationtech.geomesa.curve.BinnedTime

import scala.util.Try

/**
 * Abstraction over fetching geospatial raster data.
 *
 * @since 8/21/18
 */
trait RasterSource {
  def crs: CRS
  def extent: Extent
  def timestamp: ZonedDateTime
}
trait GeoTiffSource { _: RasterSource ⇒
  def tiffTags: TiffTags
}

object RasterSource extends LazyLogging {
  // According to https://goo.gl/2z8xx9 the header format date is 'YYYY:MM:DD HH:MM:SS'
  private val dateFormat = DateTimeFormatter.ofPattern("yyyy:MM:dd HH:mm:ss")

  case class HttpGeoTiffRasterSource(source: URI) extends RasterSource with GeoTiffSource {
    private lazy val info: GeoTiffInfo = {
      val rr = HttpRangeReader(source)
      GeoTiffReader.readGeoTiffInfo(rr, streaming = true, withOverviews = true)
    }
    def crs: CRS = info.crs
    def extent: Extent = info.extent
    lazy val tiffTags: TiffTags = {
      val rr = HttpRangeReader(source)
      TiffTags(rr)
    }
    override def timestamp: ZonedDateTime = {
      // TODO: Determine if this is the correct way to handle time.
      tiffTags.tags.headTags
        .get(Tags.TIFFTAG_DATETIME)
        .flatMap(ds ⇒ Try({
          logger.debug("Parsing header date: " + ds);
          ZonedDateTime.parse(ds, dateFormat)
        }).toOption)
        .getOrElse {
          val conn = source.toURL.openConnection()
          ZonedDateTime.ofInstant(Instant.ofEpochMilli(conn.getLastModified), ZoneOffset.UTC)
        }
    }

    override def toString: String = {
      val buf = new StringBuilder()
      buf.append(productPrefix)
      buf.append("(")
      buf.append("source=")
      buf.append(source.toASCIIString)
      buf.append(", crs=")
      buf.append(crs.toString())
      buf.append(", extent=")
      buf.append(extent.toString)
      buf.append(", timestamp=")
      buf.append(timestamp.toString)
      buf.append(", headTags=")
      buf.append(tiffTags.tags.headTags)
      buf.append(")")
      buf.toString
    }
  }
}