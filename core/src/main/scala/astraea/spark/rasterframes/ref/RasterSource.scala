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
import geotrellis.spark.tiling.LayoutDefinition
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
  def nativeTiling: Seq[Extent]
}

object RasterSource extends LazyLogging {

  /** Trait for registering a callback for logging or monitoring range reads.
   * NB: the callback will be invoked from within a Spark task, and therefore
   * is serialized along with its closure to executors. */
  trait ReadCallback extends Serializable {
    def readRange(source: RasterSource, start: Long, length: Int): Unit
  }

  private case class ReportingRangeReader(delegate: RangeReader, callback: ReadCallback, parent: RasterSource) extends RangeReader {
    override def totalLength: Long = delegate.totalLength
    override protected def readClippedRange(start: Long, length: Int): Array[Byte] = {
      callback.readRange(parent, start, length)
      delegate.readRange(start, length)
    }
  }

  def apply(source: URI, callback: Option[ReadCallback] = None): RasterSource =
    source.getScheme match {
      case "http" | "https" ⇒ HttpGeoTiffRasterSource(source, callback)
      case "file" ⇒ FileGeoTiffRasterSource(source, callback)
      case "hdfs" | "s3n" | "s3a" | "wasb" | "wasbs" ⇒
        // TODO: How can we get the active hadoop configuration
        // TODO: without having to pass it through?
        val config = SerializableConfiguration(new Configuration())
        HadoopGeoTiffRasterSource(source, config, callback)
      case "s3" ⇒ ???
      case s ⇒ throw new UnsupportedOperationException(s"Scheme '$s' not supported")
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

    def crs: CRS = tiffInfo.crs

    def extent: Extent = tiffInfo.extent

    def timestamp: Option[ZonedDateTime] = resolveDate

    def size: Long = rangeReader.totalLength

    def dimensions: (Int, Int) = tiffInfo.rasterExtent.dimensions

    def cellType: CellType = tiffInfo.cellType

    def bandCount: Int = tiffInfo.bandCount

    def nativeTiling: Seq[Extent] = {
      val segLayout = tiffInfo.segmentLayout
      val tileLayout = segLayout.tileLayout
      val layout = LayoutDefinition(extent, tileLayout)
      val transform = layout.mapTransform

      if(segLayout.isTiled) {
        for(i ← 0 until segLayout.bandSegmentCount) yield {
          val (layoutCol, layoutRow) = segLayout.getSegmentCoordinate(i)
          transform(layoutCol, layoutRow)
        }
      }
      else {
        Seq(extent)
      }
    }

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

  case class FileGeoTiffRasterSource(source: URI, callback: Option[ReadCallback]) extends RangeReaderRasterSource
    with URIRasterSource with URIRasterSourceDebugString { self ⇒
    @transient
    protected lazy val rangeReader = {
      val base = FileRangeReader(source.getPath)
      // TODO: DRY
      callback.map(cb ⇒ ReportingRangeReader(base, cb, self)).getOrElse(base)
    }
  }

  case class HadoopGeoTiffRasterSource(source: URI, config: SerializableConfiguration, callback: Option[ReadCallback]) extends RangeReaderRasterSource
    with URIRasterSource with URIRasterSourceDebugString { self ⇒
    @transient
    protected lazy val rangeReader = {
      val base = HdfsRangeReader(new Path(source.getPath), config.value)
      callback.map(cb ⇒ ReportingRangeReader(base, cb, self)).getOrElse(base)
    }
  }

  case class HttpGeoTiffRasterSource(source: URI, callback: Option[ReadCallback]) extends RangeReaderRasterSource
    with URIRasterSource with URIRasterSourceDebugString { self ⇒
    @transient
    private lazy val baseReader = HttpRangeReader(source)

    @transient
    protected lazy val rangeReader = {
      callback.map(cb ⇒ ReportingRangeReader(baseReader, cb, self)).getOrElse(baseReader)
    }

    override protected def resolveDate: Option[ZonedDateTime] = {
      super.resolveDate
        .orElse(
          baseReader.response.headers.get("Last-Modified")
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