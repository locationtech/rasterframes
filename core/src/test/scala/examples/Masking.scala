package examples

import astraea.spark.rasterframes._
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster.render._
import geotrellis.raster.{mask => _, _}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Masking extends App {

  implicit val spark = SparkSession.builder().
    master("local").appName("RasterFrames").
    config("spark.ui.enabled", "false").
    getOrCreate().
    withRasterFrames

  def readTiff(name: String): SinglebandGeoTiff = SinglebandGeoTiff(s"../samples/$name")

  val filenamePattern = "L8-B%d-Elkton-VA.tiff"
  val bandNumbers = 1 to 4
  val bandColNames = bandNumbers.map(b ⇒ s"band_$b").toArray

  val joinedRF = bandNumbers.
    map { b ⇒ (b, filenamePattern.format(b)) }.
    map { case (b, f) ⇒ (b, readTiff(f)) }.
    map { case (b, t) ⇒ t.projectedRaster.toRF(s"band_$b") }.
    reduce(_ spatialJoin _)

  val threshold = udf((t: Tile) => {
    t.convert(IntConstantNoDataCellType).map(x => if (x > 10500) x else NODATA)
  } )

  val withMaskedTile = joinedRF.withColumn("maskTile", threshold(joinedRF("band_1"))).asRF

  withMaskedTile.select(no_data_cells(withMaskedTile("maskTile"))).show()

  val masked = withMaskedTile.withColumn("masked", mask(joinedRF("band_2"), joinedRF("maskTile"))).asRF

  val maskRF = masked.toRaster(masked("masked"), 466, 428)
  val b2 = masked.toRaster(masked("band_2"), 466, 428)

  val brownToGreen = ColorRamp(
    RGBA(166,97,26,255),
    RGBA(223,194,125,255),
    RGBA(245,245,245,255),
    RGBA(128,205,193,255),
    RGBA(1,133,113,255)
  ).stops(128)

  val colors = ColorMap.fromQuantileBreaks(maskRF.tile.histogramDouble(), brownToGreen)

  maskRF.tile.color(colors).renderPng().write("mask.png")
  b2.tile.color(colors).renderPng().write("b2.png")

  spark.stop()
}
