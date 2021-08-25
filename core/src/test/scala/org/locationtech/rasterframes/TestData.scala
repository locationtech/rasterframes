/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Astraea, Inc.
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

package org.locationtech.rasterframes

import java.net.URI
import java.nio.file.{Files, Paths}
import java.time.ZonedDateTime

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster
import geotrellis.raster._
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import geotrellis.spark._
import geotrellis.spark.testkit.TileLayerRDDBuilders
import geotrellis.layer._
import geotrellis.vector._
import geotrellis.vector.io.json.JsonFeatureCollection
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.locationtech.rasterframes.expressions.tilestats.NoDataCells
import org.locationtech.rasterframes.ref.{RasterRef, RFRasterSource}
import org.locationtech.rasterframes.tiles.ProjectedRasterTile

import scala.reflect.ClassTag

/**
 * Pre-configured data constructs for testing.
 *
 * @since 4/3/17
 */
trait TestData {

  val extent = Extent(10, 20, 30, 40)
  val crs = LatLng
  val ct = ByteUserDefinedNoDataCellType(-2)
  val cols = 10
  val rows = cols
  val tileSize = cols * rows
  val tileCount = 10
  val numND = 4
  val instant = ZonedDateTime.now()
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

  def geotiffDir = {
    Paths.get(getClass.getResource("/L8-B8-Robinson-IL.tiff").getFile).getParent
  }

  def readSingleband(name: String) = SinglebandGeoTiff(IOUtils.toByteArray(getClass.getResourceAsStream("/" + name)))
  def readMultiband(name: String) = MultibandGeoTiff(IOUtils.toByteArray(getClass.getResourceAsStream("/" + name)))

  /** 774 x 500 GeoTiff */
  def sampleGeoTiff = readSingleband("L8-B8-Robinson-IL.tiff")
  /** 186 x 169 GeoTiff */
  def sampleSmallGeoTiff = readSingleband("L8-B7-Elkton-VA.tiff")

  def l8Sample(band: Int) = {
    require((1 to 11).contains(band), "Invalid band number")
    readSingleband(s"L8-B$band-Elkton-VA.tiff")
  }

  def l8SamplePath(band: Int) = {
    require((1 to 11).contains(band), "Invalid band number")
    getClass.getResource(s"/L8-B$band-Elkton-VA.tiff").toURI
  }

  def l8Labels = readSingleband("L8-Labels-Elkton-VA.tiff")

  def naipSample(band: Int) = {
    require((1 to 4).contains(band), "Invalid band number")
    readSingleband(s"NAIP-VA-b$band.tiff")
  }

  def rgbCogSample  = readMultiband("LC08_RGB_Norfolk_COG.tiff")

  def rgbCogSamplePath = getClass.getResource("/LC08_RGB_Norfolk_COG.tiff").toURI

  def sampleTileLayerRDD(implicit spark: SparkSession): TileLayerRDD[SpatialKey] = {
    val rf = sampleGeoTiff.projectedRaster.toLayer(128, 128)
    rf.toTileLayerRDD(rf.tileColumns.head).left.get
  }

  // Check the URL exists as of 2020-09-30; strictly these are not COGs because they do not have internal overviews
  private def remoteCOGSingleBand(b: Int) = URI.create(s"https://landsat-pds.s3.us-west-2.amazonaws.com/c1/L8/017/029/LC08_L1TP_017029_20200422_20200509_01_T1/LC08_L1TP_017029_20200422_20200509_01_T1_B${b}.TIF")
  lazy val remoteCOGSingleband1: URI = remoteCOGSingleBand(1)
  lazy val remoteCOGSingleband2: URI = remoteCOGSingleBand(2)

  // a public 4 band COG TIF
  lazy val remoteCOGMultiband: URI = URI.create("https://s22s-rasterframes-integration-tests.s3.amazonaws.com/m_4411708_ne_11_1_20141005.cog.tif")

  lazy val remoteMODIS: URI = URI.create("https://modis-pds.s3.amazonaws.com/MCD43A4.006/31/11/2017158/MCD43A4.A2017158.h31v11.006.2017171203421_B01.TIF")
  lazy val remoteL8: URI = URI.create("https://landsat-pds.s3.amazonaws.com/c1/L8/017/033/LC08_L1TP_017033_20181010_20181030_01_T1/LC08_L1TP_017033_20181010_20181030_01_T1_B4.TIF")
  lazy val remoteHttpMrfPath: URI = URI.create("https://s3.amazonaws.com/s22s-rasterframes-integration-tests/m_3607526_sw_18_1_20160708.mrf")
  lazy val remoteS3MrfPath: URI = URI.create("s3://naip-analytic/va/2016/100cm/rgbir/37077/m_3707764_sw_18_1_20160708.mrf")

  lazy val localSentinel: URI = getClass.getResource("/B01.jp2").toURI
  lazy val cogPath: URI = getClass.getResource("/LC08_RGB_Norfolk_COG.tiff").toURI
  lazy val singlebandCogPath: URI = getClass.getResource("/LC08_B7_Memphis_COG.tiff").toURI
  lazy val nonCogPath: URI = getClass.getResource("/L8-B8-Robinson-IL.tiff").toURI

  lazy val l8B1SamplePath: URI = l8SamplePath(1)
  lazy val l8samplePath: URI = getClass.getResource("/L8-B1-Elkton-VA.tiff").toURI
  lazy val modisConvertedMrfPath: URI = getClass.getResource("/MCD43A4.A2019111.h30v06.006.2019120033434_01.mrf").toURI

  lazy val zero = TestData.projectedRasterTile(cols, rows, 0, extent, crs, ct)
  lazy val one = TestData.projectedRasterTile(cols, rows, 1, extent, crs, ct)
  lazy val two = TestData.projectedRasterTile(cols, rows, 2, extent, crs, ct)
  lazy val three = TestData.projectedRasterTile(cols, rows, 3, extent, crs, ct)
  lazy val six = ProjectedRasterTile(three * two, three.extent, three.crs)
  lazy val nd = TestData.projectedRasterTile(cols, rows, -2, extent, crs, ct)
  lazy val randPRT = TestData.projectedRasterTile(cols, rows, scala.util.Random.nextInt(), extent, crs, ct)
  lazy val randNDPRT: Tile  = TestData.injectND(numND)(randPRT)

  lazy val randDoubleTile = TestData.projectedRasterTile(cols, rows, scala.util.Random.nextGaussian(), extent, crs, DoubleConstantNoDataCellType)
  lazy val randDoubleNDTile  = TestData.injectND(numND)(randDoubleTile)
  lazy val randPositiveDoubleTile = TestData.projectedRasterTile(cols, rows, scala.util.Random.nextDouble() + 1e-6, extent, crs, DoubleConstantNoDataCellType)

  val expectedRandNoData: Long = numND * tileCount.toLong
  val expectedRandData: Long = cols * rows * tileCount - expectedRandNoData
  lazy val randNDTilesWithNull = Seq.fill[Tile](tileCount)(TestData.injectND(numND)(
    TestData.randomTile(cols, rows, UByteConstantNoDataCellType)
  )).map(ProjectedRasterTile(_, extent, crs)) :+ null

  def rasterRef = RasterRef(RFRasterSource(TestData.l8samplePath), 0, None, None)
  def lazyPRT = rasterRef.tile


  object GeomData {
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
    lazy val geoJson = {
      import scala.collection.JavaConverters._
      val p = Paths.get(TestData.getClass
        .getResource("/L8-Labels-Elkton-VA.geojson").toURI)
      Files.readAllLines(p).asScala.mkString("\n")
    }
    lazy val features = GeomData.geoJson.parseGeoJson[JsonFeatureCollection]
      .getAllPolygonFeatures[_root_.io.circe.JsonObject]()
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
        ArrayTile(data, cols, rows).interpretAs(cellType)
      case _: DoubleCells ⇒
        val data = Array.fill(cols * rows)(rnd.nextGaussian())
        ArrayTile(data, cols, rows).interpretAs(cellType)
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
        } while (NoDataCells.op(result) != 0L)

        assert(NoDataCells.op(result) == 0L,
          s"Should not have any NoData cells for $cellType:\n${result.asciiDraw()}")
        result
    }
  }

  /** A tile created through a geometric sequence.
    * 1/n of the tile's values will equal the tile size / n, assuming 1/n exists in the sequence */
  def fracTile(cols: Int, rows: Int, binNum: Int, denom: Int = 2): Tile = {
    val fracs = (1 to binNum)
      .map(x => 1/math.pow(denom, x))
      .map(x => (cols * rows * x).toInt)
    val fracSeq = fracs.flatMap(p => (1 to p).map(_ => p))
    // fill in the rest with zeroes
    val fullArr = (fracSeq ++ Seq.fill(rows * cols - fracSeq.length)(0)).toArray
    ArrayTile(fullArr, rows, cols)
  }

  /** Create a series of random tiles. */
  val makeTiles: Int ⇒ Array[Tile] =
    count ⇒ Array.fill(count)(randomTile(4, 4, UByteCellType))

  def projectedRasterTile[N: Numeric](
    cols: Int, rows: Int,
    cellValue: => N,
    extent: Extent, crs: CRS = LatLng,
    cellType: CellType = ByteConstantNoDataCellType): ProjectedRasterTile = {
    val num = implicitly[Numeric[N]]

    val base = if(cellType.isFloatingPoint)
      ArrayTile(Array.fill(cols * rows)(num.toDouble(cellValue)), cols, rows)
    else
      ArrayTile(Array.fill(cols * rows)(num.toInt(cellValue)), cols, rows)
    ProjectedRasterTile(base.convert(cellType), extent, crs)
  }

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

    val injected = if(t.cellType.isFloatingPoint) {
      t.mapDouble((c, r, v) ⇒ (if(filter(c,r)) raster.doubleNODATA else v): Double)
    }
    else {
      t.map((c, r, v) ⇒ if(filter(c, r)) raster.NODATA else v)
    }

    injected
  }
}
