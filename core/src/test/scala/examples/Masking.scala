package examples

import org.locationtech.rasterframes._
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
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
    map { case (b, t) ⇒ t.projectedRaster.toLayer(s"band_$b") }.
    reduce(_ spatialJoin _)

  val threshold = udf((t: Tile) => {
    t.convert(IntConstantNoDataCellType).map(x => if (x > 10500) x else NODATA)
  } )

  val withMaskedTile = joinedRF.withColumn("maskTile", threshold(joinedRF("band_1"))).asLayer

  withMaskedTile.select(rf_no_data_cells(withMaskedTile("maskTile"))).show()

  val masked = withMaskedTile.withColumn("masked", rf_mask(joinedRF("band_2"), joinedRF("maskTile"))).asLayer

  val maskRF = masked.toRaster(masked("masked"), 466, 428)
  val b2 = masked.toRaster(masked("band_2"), 466, 428)

  val brownToGreen = ColorRamp(
    RGB(166,97,26),
    RGB(223,194,125),
    RGB(245,245,245),
    RGB(128,205,193),
    RGB(1,133,113)
  ).stops(128)

  val colors = ColorMap.fromQuantileBreaks(maskRF.tile.histogramDouble(), brownToGreen)

  maskRF.tile.color(colors).renderPng().write("mask.png")
  b2.tile.color(colors).renderPng().write("b2.png")

  spark.stop()
}
