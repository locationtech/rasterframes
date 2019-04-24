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

package org.locationtech.rasterframes.ref

import java.net.URI

import org.locationtech.rasterframes.NOMINAL_TILE_DIMS
import org.locationtech.rasterframes.model.TileDimensions
import RasterSource.SINGLEBAND
import com.azavea.gdal.GDALWarp
import com.typesafe.scalalogging.LazyLogging
import geotrellis.contrib.vlm.gdal.{GDALRasterSource => VLMRasterSource}
import geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource
import geotrellis.contrib.vlm.{RasterSource => GTRasterSource}
import geotrellis.proj4.CRS
import geotrellis.raster._
import geotrellis.raster.io.geotiff.Tags
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.spark.io.hadoop.HdfsRangeReader
import geotrellis.util.RangeReader
import geotrellis.vector.Extent
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.rf.RasterSourceUDT
import org.locationtech.rasterframes.model.{TileContext, TileDimensions}
import org.locationtech.rasterframes.tiles.ProjectedRasterTile
import org.locationtech.rasterframes.util.GeoTiffInfoSupport

/**
 * Abstraction over fetching geospatial raster data.
 *
 * @since 8/21/18
 */
@Experimental
sealed trait RasterSource extends ProjectedRasterLike with Serializable {
  def crs: CRS

  def extent: Extent

  def cellType: CellType

  def bandCount: Int

  def tags: Tags

  def read(bounds: GridBounds, bands: Seq[Int]): Raster[MultibandTile] =
    readBounds(Seq(bounds), bands).next()

  def read(extent: Extent, bands: Seq[Int] = SINGLEBAND): Raster[MultibandTile] =
    read(rasterExtent.gridBoundsFor(extent, clamp = true), bands)

  def readAll(dims: TileDimensions = NOMINAL_TILE_DIMS, bands: Seq[Int] = SINGLEBAND): Seq[Raster[MultibandTile]] =
    layoutBounds(dims).map(read(_, bands))

  protected def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]]

  def rasterExtent = RasterExtent(extent, cols, rows)

  def cellSize = CellSize(extent, cols, rows)

  def gridExtent = GridExtent(extent, cellSize)

  def tileContext: TileContext = TileContext(extent, crs)

  def layoutExtents(dims: TileDimensions): Seq[Extent] = {
    val re = rasterExtent
    layoutBounds(dims).map(re.rasterExtentFor).map(_.extent)
  }

  def layoutBounds(dims: TileDimensions): Seq[GridBounds] = {
    gridBounds.split(dims.cols, dims.rows).toSeq
  }
}

object RasterSource extends LazyLogging {
  final val SINGLEBAND = Seq(0)
  final val EMPTY_TAGS = Tags(Map.empty, List.empty)

  implicit def rsEncoder: ExpressionEncoder[RasterSource] = {
    RasterSourceUDT // Makes sure UDT is registered first
    ExpressionEncoder()
  }

  def apply(source: URI): RasterSource = source match {
    case IsGDAL()          => GDALRasterSource(source)
    case IsHadoopGeoTiff() =>
      // TODO: How can we get the active hadoop configuration
      // TODO: without having to pass it through?
      val config = () => new Configuration()
      HadoopGeoTiffRasterSource(source, config)
    case IsDefaultGeoTiff() => JVMGeoTiffRasterSource(source)
    case s                  => throw new UnsupportedOperationException(s"Reading '$s' not supported")
  }

  object IsGDAL {

    /** Determine if we should prefer GDAL for all types. */
    private val preferGdal: Boolean = org.locationtech.rasterframes.rfConfig.getBoolean("prefer-gdal")
    @transient
    lazy val hasGDAL: Boolean = try {
      val _ = new GDALWarp()
      true
    } catch {
      case _: UnsatisfiedLinkError =>
        logger.warn("GDAL native bindings are not available. Falling back to JVM-based reader.")
        false
    }

    val gdalOnlyExtensions = Seq(".jp2", ".mrf", ".hdf")
    def gdalOnly(source: URI): Boolean =
      if (gdalOnlyExtensions.exists(source.getPath.toLowerCase.endsWith)) {
        require(hasGDAL, s"Can only read $source if GDAL is available")
        true
      } else false

    /** Extractor for determining if a scheme indicates GDAL preference.  */
    def unapply(source: URI): Boolean =
      gdalOnly(source) || ((preferGdal || source.getScheme.startsWith("gdal+")) && hasGDAL)
  }

  object IsDefaultGeoTiff {
    def unapply(source: URI): Boolean = source.getScheme match {
      case "file" | "http" | "https" | "s3" => true
      case _                                => false
    }
  }

  object IsHadoopGeoTiff {
    def unapply(source: URI): Boolean = source.getScheme match {
      case "hdfs" | "s3n" | "s3a" | "wasb" | "wasbs" => true
      case _                                         => false
    }
  }

  case class SimpleGeoTiffInfo(
    cols: Int,
    rows: Int,
    cellType: CellType,
    extent: Extent,
    rasterExtent: RasterExtent,
    crs: CRS,
    tags: Tags,
    bandCount: Int,
    noDataValue: Option[Double]
  )

  object SimpleGeoTiffInfo {
    def apply(info: GeoTiffReader.GeoTiffInfo): SimpleGeoTiffInfo =
      SimpleGeoTiffInfo(
        info.segmentLayout.totalCols,
        info.segmentLayout.totalRows,
        info.cellType,
        info.extent,
        info.rasterExtent,
        info.crs,
        info.tags,
        info.bandCount,
        info.noDataValue
      )
  }

  trait URIRasterSource { _: RasterSource =>
    def source: URI

    abstract override def toString: String = {
      s"${getClass.getSimpleName}(${source})"
    }
  }

  /** A RasterFrames RasterSource which delegates most operations to a geotrellis-contrib RasterSource */
  abstract class DelegatingRasterSource(source: URI, delegateBuilder: () => GTRasterSource) extends RasterSource with URIRasterSource {
    @transient
    lazy val delegate = delegateBuilder()

    // Bad, bad, bad?
    override def equals(obj: Any): Boolean = obj match {
      case drs: DelegatingRasterSource => drs.source == source
      case _                           => false
    }
    override def hashCode(): Int = source.hashCode()

    // This helps reduce header reads between serializations
    lazy val info: SimpleGeoTiffInfo = {
      SimpleGeoTiffInfo(
        delegate.cols,
        delegate.rows,
        delegate.cellType,
        delegate.extent,
        delegate.rasterExtent,
        delegate.crs,
        fetchTags,
        delegate.bandCount,
        None
      )
    }

    override def cols: Int = info.cols
    override def rows: Int = info.rows
    override def crs: CRS = info.crs
    override def extent: Extent = info.extent
    override def cellType: CellType = info.cellType
    override def bandCount: Int = info.bandCount
    private def fetchTags: Tags = delegate match {
      case rs: GeoTiffRasterSource => rs.tiff.tags
      case _                       => EMPTY_TAGS
    }
    override def tags: Tags = info.tags
    override protected def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
      delegate.readBounds(bounds, bands)
    override def read(bounds: GridBounds, bands: Seq[Int]): Raster[MultibandTile] =
      delegate
        .read(bounds, bands)
        .getOrElse(throw new IllegalArgumentException(s"Bounds '$bounds' outside of source"))
    override def read(extent: Extent, bands: Seq[Int]): Raster[MultibandTile] =
      delegate
        .read(extent, bands)
        .getOrElse(throw new IllegalArgumentException(s"Extent '$extent' outside of source"))
  }

  case class JVMGeoTiffRasterSource(source: URI) extends DelegatingRasterSource(source, () => GeoTiffRasterSource(source.toASCIIString))

  case class InMemoryRasterSource(tile: Tile, extent: Extent, crs: CRS) extends RasterSource {
    def this(prt: ProjectedRasterTile) = this(prt, prt.extent, prt.crs)

    override def rows: Int = tile.rows

    override def cols: Int = tile.cols

    override def cellType: CellType = tile.cellType

    override def bandCount: Int = 1

    override def tags: Tags = EMPTY_TAGS

    override protected def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
      bounds
        .map(b => {
          val subext = rasterExtent.extentFor(b)
          Raster(MultibandTile(tile.crop(b)), subext)
        })
        .toIterator
    }
  }

  case class GDALRasterSource(source: URI) extends RasterSource with URIRasterSource {

    @transient
    private lazy val gdal = {
      val cleaned = source.toASCIIString.replace("gdal+", "")
      // VSIPath doesn't like single slash "file:/path..."
      val tweaked =
        if (cleaned.matches("^file:/[^/].*"))
          cleaned.replace("file:", "")
        else cleaned

      VLMRasterSource(tweaked)
    }

    override def crs: CRS = gdal.crs

    override def extent: Extent = gdal.extent

    private def metadata = Map.empty[String, String]

    override def cellType: CellType = gdal.cellType

    override def bandCount: Int = gdal.bandCount

    override def cols: Int = gdal.cols

    override def rows: Int = gdal.rows

    override def tags: Tags = Tags(metadata, List.empty)

    override protected def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
      gdal.readBounds(bounds, bands)
  }

  trait RangeReaderRasterSource extends RasterSource with GeoTiffInfoSupport with LazyLogging {
    protected def rangeReader: RangeReader

    private def realInfo =
      GeoTiffReader.readGeoTiffInfo(rangeReader, streaming = true, withOverviews = false)

    protected lazy val tiffInfo = SimpleGeoTiffInfo(realInfo)

    def crs: CRS = tiffInfo.crs

    def extent: Extent = tiffInfo.extent

    override def cols: Int = tiffInfo.rasterExtent.cols

    override def rows: Int = tiffInfo.rasterExtent.rows

    def cellType: CellType = tiffInfo.cellType

    def bandCount: Int = tiffInfo.bandCount

    override def tags: Tags = tiffInfo.tags

    override protected def readBounds(bounds: Traversable[GridBounds], bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
      val info = realInfo
      val geoTiffTile = GeoTiffReader.geoTiffMultibandTile(info)
      val intersectingBounds = bounds.flatMap(_.intersection(this)).toSeq
      geoTiffTile.crop(intersectingBounds, bands.toArray).map {
        case (gb, tile) =>
          Raster(tile, rasterExtent.extentFor(gb, clamp = true))
      }
    }
  }

  case class HadoopGeoTiffRasterSource(source: URI, config: () => Configuration)
      extends RangeReaderRasterSource with URIRasterSource with URIRasterSourceDebugString { self =>
    @transient
    protected lazy val rangeReader = HdfsRangeReader(new Path(source.getPath), config())
  }

  trait URIRasterSourceDebugString { _: RangeReaderRasterSource with URIRasterSource with Product =>
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
      buf.append(")")
      buf.toString
    }
  }
}
