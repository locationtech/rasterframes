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

import java.time.ZonedDateTime

import astraea.spark.rasterframes.{functions ⇒ F}
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
 * @author sfitch 
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

  def sampleGeoTiff = SinglebandGeoTiff(IOUtils.toByteArray(getClass.getResourceAsStream("/L8-B8-Robinson-IL.tiff")))

  def sampleTileLayerRDD(implicit spark: SparkSession): TileLayerRDD[SpatialKey] = {

    val raster = sampleGeoTiff.projectedRaster.reproject(LatLng)

    val layout = LayoutDefinition(LatLng.worldExtent, TileLayout(36, 18, 128, 128))

    val kb = KeyBounds(SpatialKey(0, 0), SpatialKey(layout.layoutCols, layout.layoutRows))

    val tlm = TileLayerMetadata(raster.tile.cellType, layout, layout.extent, LatLng, kb)

    val rdd = spark.sparkContext.makeRDD(Seq((raster.projectedExtent, raster.tile)))

    ContextRDD(rdd.tileToLayout(tlm), tlm)
  }
}

object TestData extends TestData {
  val rnd =  new scala.util.Random(42)

  /** Construct a tile of given size and cell type populated with random values. */
  def randomTile(cols: Int, rows: Int, cellTypeName: String): Tile = {
    val cellType = CellType.fromName(cellTypeName)
    val tile = ArrayTile.alloc(cellType, cols, rows)

    def possibleND(c: Int) =
      c == NODATA || c == byteNODATA || c == ubyteNODATA || c == shortNODATA || c == ushortNODATA

    // Initialize tile with some initial random values
    var result = tile.dualMap(_ ⇒ rnd.nextInt())(_ ⇒ rnd.nextGaussian())

    // Due to cell width narrowing and custom NoData values, we can end up randomly creating
    // NoData values. While perhaps inefficient, the safest way to ensure a tile with no-NoData values
    // with the current CellType API (GT 1.1), while still generating random data is to
    // iteratively pass through all the cells and replace NoData values as we find them.
    do {
      result = result.dualMap(
        z ⇒ if (isNoData(z)) rnd.nextInt() else z
      ) (
        z ⇒ if (isNoData(z)) rnd.nextGaussian() else z
      )
    } while (F.nodataCells(result) != 0L)

    assert(F.nodataCells(result) == 0L,
      s"Should not have any NoData cells for $cellTypeName:\n${result.asciiDraw()}")
    result
  }

  /** Create a series of random tiles. */
  val makeTiles: (Int) ⇒ Array[Tile] = (count) ⇒
    Array.fill(count)(randomTile(4, 4, "int8raw"))

  def randomSpatialTileLayerRDD(
    rasterCols: Int, rasterRows: Int,
    layoutCols: Int, layoutRows: Int)(implicit sc: SparkContext): TileLayerRDD[SpatialKey] = {
    val tile = randomTile(rasterCols, rasterRows, "uint8")
    TileLayerRDDBuilders.createTileLayerRDD(tile, layoutCols, layoutRows, LatLng)._2
  }

  def randomSpatioTemporalTileLayerRDD(
    rasterCols: Int, rasterRows: Int,
    layoutCols: Int, layoutRows: Int)(implicit sc: SparkContext): TileLayerRDD[SpaceTimeKey] = {
    val tile = randomTile(rasterCols, rasterRows, "uint8")
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
