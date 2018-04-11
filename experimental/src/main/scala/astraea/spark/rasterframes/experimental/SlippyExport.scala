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

package astraea.spark.rasterframes.experimental

import geotrellis.util.MethodExtensions
import java.net.URI

import astraea.spark.rasterframes._
import astraea.spark.rasterframes.util._
import geotrellis.proj4.WebMercator
import geotrellis.raster.io.geotiff.tags.codes.ColorSpace
import geotrellis.raster.io.geotiff._
import geotrellis.raster.resample.Bilinear
import geotrellis.raster._
import geotrellis.raster.render.{ColorMap, ColorMaps, ColorRamps}
import geotrellis.spark._
import geotrellis.spark.io.slippy.HadoopSlippyTileWriter
import geotrellis.spark.tiling.ZoomedLayoutScheme
import org.apache.spark.annotation.Experimental

import scala.util.Try

/**
 * Experimental support for exporting a RasterFrame into Slippy map format.
 *
 * @since 4/10/18
 */
@Experimental
trait SlippyExport extends MethodExtensions[RasterFrame]{
  /**
   * Export GeoTiff tiles in a slippy map directory structure; for debugging purposes only.
   * NB: Temporal components are ignored blindly.
   */
  @Experimental
  def exportGeoTiffTiles(dest: URI): Unit = {
    val spark = self.sparkSession
    implicit val sc = spark.sparkContext

    val tlm = self.tileLayerMetadata.widen
    val crs = tlm.crs
    val mapTransform = tlm.mapTransform

    val writer = new HadoopSlippyTileWriter[MultibandTile](dest.toASCIIString, "tiff")({ (key, tile) =>
      val extent = mapTransform(key)
      // If we have exactly 3 columns, we assume RGB color space.
      val opts = GeoTiffOptions.DEFAULT
        .mapWhen(_ ⇒ tile.bands.lengthCompare(3) == 0,
          _.copy(colorSpace = ColorSpace.RGB)
        )
      MultibandGeoTiff(tile, extent, crs, opts).toByteArray
    })

    val tlrdd: MultibandTileLayerRDD[SpatialKey] = self.toMultibandTileLayerRDD(self.tileColumns: _*) match {
      case Left(spatial) ⇒ spatial
      case Right(origRDD) ⇒
        val newMD = origRDD.metadata.map(_.spatialKey)
        val rdd = origRDD.map { case (k, v) ⇒ (k.spatialKey, v)}
        ContextRDD(rdd, newMD)
    }

    writer.write(0, tlrdd)
  }

  /**
   * Export tiles as a slippy map. For debugging purposes only.
   * NB: Temporal components are ignored blindly.
   */
  @Experimental
  def exportSlippyMap(dest: URI, colorMap: Option[ColorMap] = None): Unit = {
    val spark = self.sparkSession
    implicit val sc = spark.sparkContext

    val inputRDD: MultibandTileLayerRDD[SpatialKey] = self.toMultibandTileLayerRDD(self.tileColumns: _*) match {
      case Left(spatial) ⇒ spatial
      case Right(origRDD) ⇒
        val newMD = origRDD.metadata.map(_.spatialKey)
        val rdd = origRDD.map { case (k, v) ⇒ (k.spatialKey, v)}
        ContextRDD(rdd, newMD)
    }

    val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)

    val (zoom, reprojected) = inputRDD.reproject(WebMercator, layoutScheme, Bilinear)

    val writer = new HadoopSlippyTileWriter[MultibandTile](dest.toASCIIString, "png")({ (_, tile) =>
      val png = if(colorMap.isEmpty && tile.bands.lengthCompare(3) == 0) {
        // `Try` below is due to https://github.com/locationtech/geotrellis/issues/2621
        tile.mapBands((_, t) ⇒ Try(t.rescale(0, 255)).getOrElse(t)).renderPng()
      }
      else {
        // Are there other ways to use the other bands?
        val selected = tile.bands.head
        colorMap.map(m ⇒ selected.renderPng(m)).getOrElse(selected.renderPng(ColorRamps.greyscale(256)))
      }
      png.bytes
    })

    writer.write(zoom, reprojected)
  }
}

object SlippyExport {
  implicit class RasterFrameHasSlippy(val self: RasterFrame) extends SlippyExport
}

