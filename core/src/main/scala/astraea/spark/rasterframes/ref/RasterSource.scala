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

import astraea.spark.rasterframes.tiles.ProjectedRasterTile
import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff.{ArraySegmentBytes, GeoTiffOptions, GeoTiffSegmentLayout, MultibandGeoTiff, SinglebandGeoTiff, Tags}
import geotrellis.raster.{CellSize, CellType, GridExtent, MultibandTile, Raster, RasterExtent, Tile, TileLayout}
import geotrellis.spark.io.hadoop.HdfsRangeReader
import geotrellis.spark.io.s3.S3Client
import geotrellis.spark.io.s3.util.S3RangeReader
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
sealed trait RasterSource extends ProjectedRasterLike with Serializable {
  def crs: CRS
  def extent: Extent
  def timestamp: Option[ZonedDateTime]
  def size: Long
  def dimensions: (Int, Int)
  def cellType: CellType
  def bandCount: Int
  def read(extent: Extent): Either[Raster[Tile], Raster[MultibandTile]]
  def nativeLayout: Option[TileLayout]
  def cols: Int = dimensions._1
  def rows: Int = dimensions._2
  def rasterExtent = RasterExtent(extent, cols, rows)
  def cellSize = CellSize(extent, cols, rows)
  def gridExtent = GridExtent(extent, cellSize)

  def nativeTiling: Seq[Extent] = {
    nativeLayout.map { tileLayout  ⇒
      val layout = LayoutDefinition(extent, tileLayout)
      val transform = layout.mapTransform
      for {
        col ← 0 until tileLayout.layoutCols
        row ← 0 until tileLayout.layoutRows
      } yield transform(col, row)
    }
      .getOrElse(Seq(extent))
  }
}

object RasterSource extends LazyLogging {

  def apply(source: URI, callback: Option[ReadCallback] = None): RasterSource =
    source.getScheme match {
      case "http" | "https" ⇒ HttpGeoTiffRasterSource(source, callback)
      case "file" ⇒ FileGeoTiffRasterSource(source, callback)
      case "hdfs" | "s3n" | "s3a" | "wasb" | "wasbs" ⇒
        // TODO: How can we get the active hadoop configuration
        // TODO: without having to pass it through?
        val config = () ⇒ new Configuration()
        HadoopGeoTiffRasterSource(source, config, callback)
      case "s3" ⇒
        val client = () ⇒ S3Client.DEFAULT
        S3GeoTiffRasterSource(source, client, callback)
      case s ⇒ throw new UnsupportedOperationException(s"Scheme '$s' not supported")
    }


  case class SimpleGeoTiffInfo(
    cellType: CellType,
    extent: Extent,
    rasterExtent: RasterExtent,
    crs: CRS,
    tags: Tags,
    segmentLayout: GeoTiffSegmentLayout,
    bandCount: Int,
    noDataValue: Option[Double]
  )

  object SimpleGeoTiffInfo {
    def apply(info: GeoTiffReader.GeoTiffInfo): SimpleGeoTiffInfo =
      SimpleGeoTiffInfo(info.cellType, info.extent, info.rasterExtent, info.crs, info.tags, info.segmentLayout, info.bandCount, info.noDataValue)
  }

  // According to https://goo.gl/2z8xx9 the GeoTIFF date format is 'YYYY:MM:DD HH:MM:SS'
  private val dateFormat = DateTimeFormatter.ofPattern("yyyy:MM:dd HH:mm:ss")

  trait URIRasterSource { _: RasterSource ⇒
    def source: URI

    abstract override def toString: String = {
      s"${getClass.getSimpleName}(${source})"
    }
  }


  case class InMemoryRasterSource(tile: Tile, extent: Extent, crs: CRS) extends RasterSource {
    def this(prt: ProjectedRasterTile) = this(prt, prt.extent, prt.crs)
    override def timestamp: Option[ZonedDateTime] = None
    override def size: Long = tile.size
    override def dimensions: (Int, Int) = tile.dimensions
    override def cellType: CellType = tile.cellType
    override def bandCount: Int = 1
    override def read(extent: Extent): Either[Raster[Tile], Raster[MultibandTile]] = Left(
      Raster(tile.crop(rasterExtent.gridBoundsFor(extent, false)), extent)
    )
    override def nativeLayout: Option[TileLayout] = Some(TileLayout(1, 1, cols, rows))
  }

  trait RangeReaderRasterSource extends RasterSource {
    protected def rangeReader: RangeReader

    private def realInfo =
      GeoTiffReader.readGeoTiffInfo(rangeReader, streaming = true, withOverviews = false)

    private lazy val tiffInfo = SimpleGeoTiffInfo(realInfo)

    def crs: CRS = tiffInfo.crs

    def extent: Extent = tiffInfo.extent

    def timestamp: Option[ZonedDateTime] = resolveDate

    def size: Long = rangeReader.totalLength

    def dimensions: (Int, Int) = tiffInfo.rasterExtent.dimensions

    override def cols: Int = tiffInfo.rasterExtent.cols

    override def rows: Int = tiffInfo.rasterExtent.rows

    def cellType: CellType = tiffInfo.cellType

    def bandCount: Int = tiffInfo.bandCount

    def nativeLayout: Option[TileLayout] = {
      if(tiffInfo.segmentLayout.isTiled)
        Some(tiffInfo.segmentLayout.tileLayout)
      else None
    }

    // TODO: Determine if this is the correct way to  handle time.
    protected def resolveDate: Option[ZonedDateTime] = {
      tiffInfo.tags.headTags
        .get(Tags.TIFFTAG_DATETIME)
        .flatMap(ds ⇒ Try({
          logger.debug("Parsing header date: " + ds)
          ZonedDateTime.parse(ds, dateFormat)
        }).toOption)
    }

    def read(extent: Extent): Either[Raster[Tile], Raster[MultibandTile]] = {
      val info = realInfo
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

  case class HadoopGeoTiffRasterSource(source: URI, config: () ⇒ Configuration, callback: Option[ReadCallback]) extends RangeReaderRasterSource
    with URIRasterSource with URIRasterSourceDebugString { self ⇒
    @transient
    protected lazy val rangeReader = {
      val base = HdfsRangeReader(new Path(source.getPath), config())
      callback.map(cb ⇒ ReportingRangeReader(base, cb, self)).getOrElse(base)
    }
  }

  case class S3GeoTiffRasterSource(source: URI, client: () ⇒ S3Client, callback: Option[ReadCallback]) extends RangeReaderRasterSource
    with URIRasterSource with URIRasterSourceDebugString { self ⇒
    @transient
    protected lazy val rangeReader = {
      val base = S3RangeReader(source, client())
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