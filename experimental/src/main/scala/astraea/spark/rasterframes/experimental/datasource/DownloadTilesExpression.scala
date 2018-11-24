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

package astraea.spark.rasterframes.experimental.datasource

import java.net.URI

import astraea.spark.rasterframes.encoders.CatalystSerializer._
import astraea.spark.rasterframes.ref.HttpRangeReader
import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.CRS
import geotrellis.raster.{ProjectedRaster, Tile}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.spark.io.hadoop.HdfsRangeReader
import geotrellis.spark.io.s3.S3Client
import geotrellis.spark.io.s3.util.S3RangeReader
import geotrellis.util.{ByteReader, FileRangeReader, RangeReader, StreamingByteReader}
import geotrellis.vector.Extent
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, Generator, Literal}
import org.apache.spark.sql.rf._
import org.apache.spark.sql.types._

/**
 * Catalyst generator to convert a geotiff download URL into a series of rows containing the internal
 * tiles and associated extents.
 *
 * @since 5/4/18
 */
case class DownloadTilesExpression(children: Seq[Expression]) extends Expression
  with Generator with CodegenFallback with DownloadSupport with LazyLogging {

  private val TileType = new TileUDT()

  override def nodeName: String = "download_tiles"

  def inputTypes = Seq.fill(children.length)(StringType)

  override def elementSchema: StructType = StructType(Seq(
    StructField("crs", classOf[CRS].schema, true),
    StructField("extent", classOf[Extent].schema, true)
  ) ++
    children
      .zipWithIndex
      .map {
        case (l: Literal, _) ⇒ String.valueOf(l.value)
        case (a: Alias, _) ⇒ a.name
        case (_, i) ⇒ s"_${i + 1}"
      }
      .map(name ⇒ {
        StructField(name, TileType, true)
      })
  )

  private def reader(uri: URI): ByteReader = {
    val rr: RangeReader = uri.getScheme match {
      case "http" | "https" ⇒ HttpRangeReader(uri)
      case "file" ⇒ FileRangeReader(uri.getPath)
      case "hdfs" | "s3n" | "s3a" | "wasb" | "wasbs" ⇒
        HdfsRangeReader(new Path(uri), new Configuration())
      case "s3" ⇒
        S3RangeReader(uri, S3Client.DEFAULT)
    }
    StreamingByteReader(rr)
  }

  def checkDimensions(tiles: Array[Array[ProjectedRaster[Tile]]], uris: Seq[URI]) = {
    val rowDims = tiles.map(row ⇒ {
      row.map {
        case null ⇒ null
        case t ⇒ t.tile.dimensions
      }
    })

    val notSameDim = rowDims
      .map(_.distinct.count(_ != null))
      .indexWhere(_ > 1)

    if (notSameDim >= 0) {
      throw new IllegalArgumentException("Detecuris: Seq[Option[String]] ted rows with different sized tiles: " +
        rowDims(notSameDim).mkString(", ") + "\nfrom: " + uris.mkString(", "))
    }
  }


  private def safeTranspose(tiles: Seq[Seq[ProjectedRaster[Tile]]]) = {
    // We need to transpose the nested sequence of from column major to row major order
    // However, some columns have no rows, the default transpose needs a regular grid...
    // so we do it manually.
    val numRows = tiles.map(_.size).max
    val numCols = tiles.length
    val tileGrid = Array.fill(numRows)(Array.ofDim[ProjectedRaster[Tile]](numCols))

    for {
      (rows, col) ← tiles.zipWithIndex
      (tile, row) ← rows.zipWithIndex
    } {
      tileGrid(row)(col) = tile
    }
    tileGrid
  }

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    val uris = children.map(_.eval(input).toString).map(URI.create)
    val colMajor = uris.map { uri ⇒
      val tiff = GeoTiffReader.readSingleband(reader(uri))
      val segmentLayout = tiff.imageData.segmentLayout
      val windows = segmentLayout.listWindows(256)
      val re = tiff.rasterExtent

      val subtiles = tiff.crop(windows)
      for {
        (gridbounds, tile) ← subtiles.toSeq
      } yield {
        val extent = re.extentFor(gridbounds, false)
        ProjectedRaster(tile, extent, tiff.crs)
      }
    }

    val rowMajor = safeTranspose(colMajor)

    checkDimensions(rowMajor, uris)

    rowMajor.map(row ⇒ {
      val serializedTiles = row.map {
        case null ⇒ null
        case t ⇒ TileType.serialize(t)
      }

      val firstBandExt = row
        .find(_ != null)
        .map(p ⇒ Seq(p.crs.toInternalRow, p.extent.toInternalRow))
        .getOrElse(Seq(null, null))

      InternalRow(firstBandExt ++ serializedTiles: _*)
    })
  }

}

object DownloadTilesExpression {
  def apply(urls: Seq[Column]): Column =  new Column(
    new DownloadTilesExpression(urls.map(_.expr))
  )
}
