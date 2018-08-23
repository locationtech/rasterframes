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

import java.net.URI
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset, ZonedDateTime}

import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff, Tags}
import geotrellis.raster.{CellSize, CellType, GridExtent, MultibandTile, Raster, RasterExtent, Tile}
import geotrellis.vector.Extent
import scalaj.http.HttpResponse

import scala.util.Try

/**
 * Abstraction over fetching geospatial raster data.
 *
 * @since 8/21/18
 */
trait RasterSource extends Serializable {
  def crs: CRS
  def extent: Extent
  def timestamp: Option[ZonedDateTime]
  def size: Long
  def dimensions: (Int, Int)
  def cellType: CellType
  def bandCount: Int
  def read(extent: Extent): Either[Raster[Tile], Raster[MultibandTile]]
  def cols: Int = dimensions._1
  def rows: Int = dimensions._2
  def rasterExtent = RasterExtent(extent, cols, rows)
  def cellSize = CellSize(extent, cols, rows)
  def gridExtent = GridExtent(extent, cellSize)
}

trait URIRasterSource extends RasterSource {
  def source: URI
}

object RasterSource extends LazyLogging {

  def apply(source: URI): URIRasterSource = source.getScheme match {
    case "http" | "https" ⇒ HttpGeoTiffRasterSource(source)
    case "hdfs" ⇒ ???
    case "s3" ⇒ ???
    case _ ⇒ ???
  }

  // According to https://goo.gl/2z8xx9 the GeoTIFF date format is 'YYYY:MM:DD HH:MM:SS'
  private val dateFormat = DateTimeFormatter.ofPattern("yyyy:MM:dd HH:mm:ss")

  case class HttpGeoTiffRasterSource(source: URI) extends URIRasterSource {
    @transient
    private lazy val rangeReader = HttpRangeReader(source)

    @transient
    private lazy val tiffInfo =
      GeoTiffReader.readGeoTiffInfo(rangeReader, streaming = true, withOverviews = false)

    // TODO: Determine if this is the correct way to handle time.
    private def resolveDate(tags: Map[String, String], httpResponse: ⇒ HttpResponse[_]) = {
      tags
        .get(Tags.TIFFTAG_DATETIME)
        .flatMap(ds ⇒ Try({
          logger.debug("Parsing header date: " + ds)
          ZonedDateTime.parse(ds, dateFormat)
        }).toOption)
        .orElse(
          httpResponse.headers.get("Last-Modified")
            .flatMap(_.headOption)
            .flatMap(s ⇒ Try(ZonedDateTime.parse(s, DateTimeFormatter.RFC_1123_DATE_TIME)).toOption)
        )
    }

    // Batched lazy evaluation
    lazy val (crs, extent, timestamp, size, cellType, bandCount, dimensions) = (
      tiffInfo.crs,
      tiffInfo.extent,
      resolveDate(tiffInfo.tags.headTags, rangeReader.response),
      rangeReader.totalLength,
      tiffInfo.cellType,
      tiffInfo.bandCount,
      tiffInfo.rasterExtent.dimensions
    )

    def read(extent: Extent): Either[Raster[Tile], Raster[MultibandTile]] = {
      val info = tiffInfo
      if (bandCount == 1) {
        val geoTiffTile = GeoTiffReader.geoTiffSinglebandTile(info)
        val gt = new SinglebandGeoTiff(
          geoTiffTile,
          info.extent,
          info.crs,
          info.tags,
          info.options,
          List.empty
        )
        Left(gt.crop(extent).raster)
      }
      else {
        val geoTiffTile = GeoTiffReader.geoTiffMultibandTile(info)
        val gt = new MultibandGeoTiff(
          geoTiffTile,
          info.extent,
          info.crs,
          info.tags,
          info.options,
          List.empty
        )
        Right(gt.crop(extent).raster)
      }
    }

    def toDebugString: String = {
      val buf = new StringBuilder()
      buf.append(productPrefix)
      buf.append("(")
      buf.append("source=")
      buf.append(source.toASCIIString)
      buf.append(", size=")
      buf.append(size)
      buf.append(", dimensions=")
      buf.append(dimensions)
      buf.append(", crs=")
      buf.append(crs)
      buf.append(", extent=")
      buf.append(extent)
      buf.append(", timestamp=")
      buf.append(timestamp)
      buf.append(")")
      buf.toString
    }
  }

}