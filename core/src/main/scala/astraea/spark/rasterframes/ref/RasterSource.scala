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
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff, Tags}
import geotrellis.raster.{CellSize, CellType, GridExtent, MultibandTile, Raster, RasterExtent, Tile}
import geotrellis.spark.io.hadoop.{HdfsRangeReader, SerializableConfiguration}
import geotrellis.util.{FileRangeReader, RangeReader}
import geotrellis.vector.Extent
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

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

object RasterSource extends LazyLogging {

  def apply(source: URI): RasterSource = source.getScheme match {
    case "http" | "https" ⇒ HttpGeoTiffRasterSource(source)
    case "file" ⇒ FileGeoTiffRasterSource(source)
    case "hdfs" | "s3n" | "s3a" | "wasb" | "wasbs" ⇒
      // TODO: How can we get the active hadoop configuration without having to pass it through?
      val config = SerializableConfiguration(new Configuration())
      HadoopGeoTiffRasterSource(source, config)
    case "s3" ⇒ ???
    case _ ⇒ ???
  }

  // According to https://goo.gl/2z8xx9 the GeoTIFF date format is 'YYYY:MM:DD HH:MM:SS'
  private val dateFormat = DateTimeFormatter.ofPattern("yyyy:MM:dd HH:mm:ss")

  trait URIRasterSource { _: RasterSource ⇒
    def source: URI
  }

  trait RangeReaderRasterSource extends RasterSource {
    protected def rangeReader: RangeReader

    @transient
    private lazy val tiffInfo: GeoTiffReader.GeoTiffInfo =
      GeoTiffReader.readGeoTiffInfo(rangeReader, streaming = true, withOverviews = false)

    override def crs: CRS = tiffInfo.crs

    override def extent: Extent = tiffInfo.extent

    override def timestamp: Option[ZonedDateTime] = resolveDate

    override def size: Long = rangeReader.totalLength

    override def dimensions: (Int, Int) = tiffInfo.rasterExtent.dimensions

    override def cellType: CellType = tiffInfo.cellType

    override def bandCount: Int = tiffInfo.bandCount

    // TODO: Determine if this is the correct way to handle time.
    protected def resolveDate: Option[ZonedDateTime] = {
      tiffInfo.tags.headTags
        .get(Tags.TIFFTAG_DATETIME)
        .flatMap(ds ⇒ Try({
          logger.debug("Parsing header date: " + ds)
          ZonedDateTime.parse(ds, dateFormat)
        }).toOption)
    }

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
  }

  case class FileGeoTiffRasterSource(source: URI) extends RangeReaderRasterSource
    with URIRasterSource with URIRasterSourceDebugString {
    @transient
    protected lazy val rangeReader = FileRangeReader(source.getPath)
  }

  case class HadoopGeoTiffRasterSource(source: URI, config: SerializableConfiguration) extends RangeReaderRasterSource
    with URIRasterSource with URIRasterSourceDebugString {
    @transient
    protected lazy val rangeReader = HdfsRangeReader(new Path(source.getPath), config.value)
  }

  case class HttpGeoTiffRasterSource(source: URI) extends RangeReaderRasterSource
    with URIRasterSource with URIRasterSourceDebugString {
    @transient
    protected lazy val rangeReader = HttpRangeReader(source)

    override protected def resolveDate: Option[ZonedDateTime] = {
      super.resolveDate
        .orElse(
          rangeReader.response.headers.get("Last-Modified")
            .flatMap(_.headOption)
            .flatMap(s ⇒ Try(ZonedDateTime.parse(s, DateTimeFormatter.RFC_1123_DATE_TIME)).toOption)
        )
    }
  }

  trait URIRasterSourceDebugString { _: RangeReaderRasterSource with URIRasterSource with Product ⇒
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