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

package org.locationtech.rasterframes.ref

import java.net.URI
import com.github.blemale.scaffeine.Scaffeine
import com.typesafe.scalalogging.LazyLogging
import frameless.Injection
import geotrellis.proj4.CRS
import geotrellis.raster._
import geotrellis.raster.io.geotiff.Tags
import geotrellis.vector.Extent
import org.apache.hadoop.conf.Configuration
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.rf.{RasterSourceUDT}
import org.locationtech.rasterframes.model.TileContext
import org.locationtech.rasterframes.util.KryoSupport
import org.locationtech.rasterframes.{NOMINAL_TILE_DIMS, rfConfig}

import java.nio.ByteBuffer
import scala.concurrent.duration.{Duration, FiniteDuration}

/**
 * Abstraction over fetching geospatial raster data.
 *
 * @since 8/21/18
 */
@Experimental
abstract class RFRasterSource extends CellGrid[Int] with ProjectedRasterLike with Serializable {
  import RFRasterSource._

  def crs: CRS

  def extent: Extent

  def cellType: CellType

  def bandCount: Int

  def tags: Tags

  def read(bounds: GridBounds[Int], bands: Seq[Int]): Raster[MultibandTile] =
    readBounds(Seq(bounds), bands).next()

  def read(extent: Extent, bands: Seq[Int] = SINGLEBAND): Raster[MultibandTile] =
    read(rasterExtent.gridBoundsFor(extent, clamp = true), bands)

  def readAll(dims: Dimensions[Int] = NOMINAL_TILE_DIMS, bands: Seq[Int] = SINGLEBAND): Seq[Raster[MultibandTile]] =
    layoutBounds(dims).map(read(_, bands))

  def readBounds(bounds: Traversable[GridBounds[Int]], bands: Seq[Int]): Iterator[Raster[MultibandTile]]

  def rasterExtent = RasterExtent(extent, cols, rows)

  def cellSize = CellSize(extent, cols, rows)

  def gridExtent: GridExtent[Int] = GridExtent[Int](extent, cellSize)

  def gridBounds: GridBounds[Int] = GridBounds(0, 0, cols - 1, rows - 1)

  def tileContext: TileContext = TileContext(extent, crs)

  def layoutExtents(dims: Dimensions[Int]): Seq[Extent] = {
    val re = rasterExtent
    layoutBounds(dims).map(re.extentFor(_, clamp = true))
  }

  def layoutBounds(dims: Dimensions[Int]): Seq[GridBounds[Int]] = {
    gridBounds.split(dims.cols, dims.rows).toSeq
  }
}

object RFRasterSource extends LazyLogging {
  final val SINGLEBAND = Seq(0)
  final val EMPTY_TAGS = Tags(Map.empty, List.empty)

  val cacheTimeout: FiniteDuration = Duration.fromNanos(rfConfig.getDuration("raster-source-cache-timeout").toNanos)

  private[ref] val rsCache = Scaffeine()
    .recordStats()
    .expireAfterAccess(RFRasterSource.cacheTimeout)
    .build[String, RFRasterSource]

  def cacheStats = rsCache.stats()

  implicit def rsEncoder: ExpressionEncoder[RFRasterSource] = {
    RasterSourceUDT // Makes sure UDT is registered first
    ExpressionEncoder()
  }

  def apply(source: URI): RFRasterSource =
    rsCache.get(
      source.toASCIIString, _ => source match {
        case IsGDAL()          =>
          if (rfConfig.getBoolean("jp2-gdal-thread-lock") && source.getPath.toLowerCase().endsWith("jp2"))
            JP2GDALRasterSource(source)
          else
            GDALRasterSource(source)
        case IsHadoopGeoTiff() =>
          // TODO: How can we get the active hadoop configuration
          // TODO: without having to pass it through?
          val config = () => new Configuration()
          HadoopGeoTiffRasterSource(source, config)
        case IsDefaultGeoTiff() => JVMGeoTiffRasterSource(source)
        case s                  => throw new UnsupportedOperationException(s"Reading '$s' not supported")
      }
    )

  object IsGDAL {

    /** Determine if we should prefer GDAL for all types. */
    private val preferGdal: Boolean = org.locationtech.rasterframes.rfConfig.getBoolean("prefer-gdal")

    val gdalOnlyExtensions = Seq(".jp2", ".mrf", ".hdf", ".vrt")

    def gdalOnly(source: URI): Boolean =
      if (gdalOnlyExtensions.exists(source.getPath.toLowerCase.endsWith)) {
        require(GDALRasterSource.hasGDAL, s"Can only read $source if GDAL is available")
        true
      } else false

    /** Extractor for determining if a scheme indicates GDAL preference.  */
    def unapply(source: URI): Boolean = {
      lazy val schemeIsGdal = Option(source.getScheme)
        .exists(_.startsWith("gdal"))

      gdalOnly(source) || ((preferGdal || schemeIsGdal) && GDALRasterSource.hasGDAL)
    }
  }

  object IsDefaultGeoTiff {
    def unapply(source: URI): Boolean = source.getScheme match {
      case "file" | "http" | "https" | "s3" => true
      case null | ""                        => true
      case _                                => false
    }
  }

  object IsHadoopGeoTiff {
    def unapply(source: URI): Boolean = source.getScheme match {
      case "hdfs" | "s3n" | "s3a" | "wasb" | "wasbs" => true
      case _                                         => false
    }
  }

  trait URIRasterSource { _: RFRasterSource =>
    def source: URI

    abstract override def toString: String = {
      s"${getClass.getSimpleName}(${source})"
    }
  }
  trait URIRasterSourceDebugString { _: RFRasterSource with URIRasterSource with Product =>
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
