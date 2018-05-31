/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright (c) 2017. Astraea, Inc.
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
 */

package astraea.spark.rasterframes

import java.nio.file.Path
import java.time.ZonedDateTime

import astraea.spark.rasterframes.{functions ⇒ F}
import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import geotrellis.proj4.LatLng
import geotrellis.raster
import geotrellis.raster._
import geotrellis.raster.io.geotiff.{GeoTiff, SinglebandGeoTiff}
import geotrellis.spark.testkit.TileLayerRDDBuilders
import geotrellis.spark.tiling.{CRSWorldExtent, LayoutDefinition}
import geotrellis.spark._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag
import scala.util.Random

/**
 * Pre-configured data constructs for testing.
 *
 * @since 4/3/17
 */
trait TestData {
  val instant = ZonedDateTime.now()
  val extent = Extent(1, 2, 3, 4)
  val sk = SpatialKey(37, 41)
  val stk = SpaceTimeKey(sk, instant)
  val pe = ProjectedExtent(extent, LatLng)
  val tpe = TemporalProjectedExtent(pe, instant)
  val tlm = TileLayerMetadata(
    CellType.fromName("uint8"),
    LayoutDefinition(
      extent,
      TileLayout(
        4, 4, 4, 4
      )
    ),
    extent, LatLng, KeyBounds(stk, stk)
  )

  def squareIncrementingTile(size: Int): Tile = ByteArrayTile((1 to (size * size)).map(_.toByte).toArray, size, size)

  val byteArrayTile: Tile = squareIncrementingTile(3)
  val maskingTile: Tile = ByteArrayTile(Array[Byte](-4, -4, -4, byteNODATA, byteNODATA, byteNODATA, 15, 15, 15), 3, 3)
  val bitConstantTile = BitConstantTile(1, 2, 2)
  val byteConstantTile = ByteConstantTile(7, 3, 3)

  val multibandTile = MultibandTile(byteArrayTile, byteConstantTile)

  def rangeArray[T: ClassTag](size: Int, conv: (Int ⇒ T)): Array[T] =
    (1 to size).map(conv).toArray

  val allTileTypes: Seq[Tile] = {
    val rows = 3
    val cols = 3
    val size = rows * cols
    Seq(
      BitArrayTile(Array[Byte](0,1,2,3,4,5,6,7,8), 3*8, 3),
      ByteArrayTile(rangeArray(size, _.toByte), rows, cols),
      DoubleArrayTile(rangeArray(size, _.toDouble), rows, cols),
      FloatArrayTile(rangeArray(size, _.toFloat), rows, cols),
      IntArrayTile(rangeArray(size, identity), rows, cols),
      ShortArrayTile(rangeArray(size, _.toShort), rows, cols),
      UByteArrayTile(rangeArray(size, _.toByte), rows, cols),
      UShortArrayTile(rangeArray(size, _.toShort), rows, cols)
    )

  }

  def readSingleband(name: String) = SinglebandGeoTiff(IOUtils.toByteArray(getClass.getResourceAsStream("/" + name)))

  /** 774 x 500 GeoTiff */
  def sampleGeoTiff = readSingleband("L8-B8-Robinson-IL.tiff")
  /** 186 x 169 GeoTiff */
  def sampleSmallGeoTiff = readSingleband("L8-B7-Elkton-VA.tiff")

  def l8Sample(band: Int) = {
    require((1 to 11).contains(band), "Invalid band number")
    readSingleband(s"L8-B$band-Elkton-VA.tiff")
  }
  def l8Labels = readSingleband("L8-Labels-Elkton-VA.tiff")

  def naipSample(band: Int) = {
    require((1 to 4).contains(band), "Invalid band number")
    readSingleband(s"NAIP-VA-b$band.tiff")
  }

  def sampleTileLayerRDD(implicit spark: SparkSession): TileLayerRDD[SpatialKey] = {
    val rf = sampleGeoTiff.projectedRaster.toRF(128, 128)
    rf.toTileLayerRDD(rf.tileColumns.head).left.get
  }

  object JTS {
    val fact = new GeometryFactory()
    val c1 = new Coordinate(1, 2)
    val c2 = new Coordinate(3, 4)
    val c3 = new Coordinate(5, 6)
    val point = fact.createPoint(c1)
    val line = fact.createLineString(Array(c1, c2))
    val poly = fact.createPolygon(Array(c1, c2, c3, c1))
    val mpoint = fact.createMultiPoint(Array(point, point, point))
    val mline = fact.createMultiLineString(Array(line, line, line))
    val mpoly = fact.createMultiPolygon(Array(poly, poly, poly))
    val coll = fact.createGeometryCollection(Array(point, line, poly, mpoint, mline, mpoly))
    val all = Seq(point, line, poly, mpoint, mline, mpoly, coll)
  }
}

object TestData extends TestData {
  val rnd =  new scala.util.Random(42)

  /** Construct a tile of given size and cell type populated with random values. */
  def randomTile(cols: Int, rows: Int, cellType: CellType): Tile = {
    // Initialize tile with some initial random values
    val base: Tile = cellType match {
      case _: FloatCells ⇒
        val data = Array.fill(cols * rows)(rnd.nextGaussian().toFloat)
        ArrayTile(data, cols, rows)
      case _: DoubleCells ⇒
        val data = Array.fill(cols * rows)(rnd.nextGaussian())
        ArrayTile(data, cols, rows)
      case _ ⇒
        val words = cellType.bits / 8
        val bytes = Array.ofDim[Byte](cols * rows * words)
        rnd.nextBytes(bytes)
        ArrayTile.fromBytes(bytes, cellType, cols, rows)
    }

    cellType match {
      case _: NoNoData ⇒ base
      case _ ⇒
        // Due to cell width narrowing and custom NoData values, we can end up randomly creating
        // NoData values. While perhaps inefficient, the safest way to ensure a tile with no-NoData values
        // with the current CellType API (GT 1.1), while still generating random data is to
        // iteratively pass through all the cells and replace NoData values as we find them.
        var result = base
        do {
          result = result.dualMap(
            z ⇒ if (isNoData(z)) rnd.nextInt(1 << cellType.bits) else z
          ) (
            z ⇒ if (isNoData(z)) rnd.nextGaussian() else z
          )
        } while (F.noDataCells(result) != 0L)

        assert(F.noDataCells(result) == 0L,
          s"Should not have any NoData cells for $cellType:\n${result.asciiDraw()}")
        result
    }
  }

  def binomialTile(cols: Int, rows: Int, p: Double): Tile = {
    val num = (cols * rows) - 1
    require(p > 0 && p < 1)
    def binomial(n: Int, k: Int): Int = {
      require(n >= 0 && k >= 0, "Greater than 0!")
      if (k == 0) {
        1
      }
      else if (k > n/2) {
        binomial(n, n - k)
      }
      else {
        n * binomial(n - 1, k - 1) / k
      }
    }

    val binArr = (0 to num).map(k => binomial(num, k) * math.pow(p, k) * math.pow(1 - p, num - k)).toArray
    ArrayTile(binArr, cols, rows)
  }

  /** Create a series of random tiles. */
  val makeTiles: (Int) ⇒ Array[Tile] = (count) ⇒
    Array.fill(count)(randomTile(4, 4, UByteCellType))

  def randomSpatialTileLayerRDD(
    rasterCols: Int, rasterRows: Int,
    layoutCols: Int, layoutRows: Int)(implicit sc: SparkContext): TileLayerRDD[SpatialKey] = {
    val tile = randomTile(rasterCols, rasterRows, UByteCellType)
    TileLayerRDDBuilders.createTileLayerRDD(tile, layoutCols, layoutRows, LatLng)._2
  }

  def randomSpatioTemporalTileLayerRDD(
    rasterCols: Int, rasterRows: Int,
    layoutCols: Int, layoutRows: Int)(implicit sc: SparkContext): TileLayerRDD[SpaceTimeKey] = {
    val tile = randomTile(rasterCols, rasterRows, UByteCellType)
    val tileLayout = TileLayout(layoutCols, layoutRows, rasterCols/layoutCols, rasterRows/layoutRows)
    TileLayerRDDBuilders.createSpaceTimeTileLayerRDD(Seq((tile, ZonedDateTime.now())), tileLayout, tile.cellType)
  }

  def injectND(num: Int)(t: Tile): Tile = {
    val indexes = List.tabulate(t.size)(identity)
    val targeted = rnd.shuffle(indexes).take(num)
    def filter(c: Int, r: Int) = targeted.contains(r * t.cols + c)

    if(t.cellType.isFloatingPoint) {
      t.mapDouble((c, r, v) ⇒ {
        if(filter(c,r)) raster.doubleNODATA else v
      })
    }
    else {
      t.map((c, r, v) ⇒ {
        if(filter(c, r)) raster.NODATA else v
      })
    }
  }
}
