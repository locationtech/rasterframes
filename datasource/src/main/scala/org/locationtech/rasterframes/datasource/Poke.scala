package org.locationtech.rasterframes.datasource
import _root_.geotrellis.raster._

object Poke extends App {
//  import _root_.geotrellis.raster.io.geotiff.TiffType
//  val enc = TiffType.tiffTypeEncoder
//  println(enc(TiffType.fromCode(43)))

  val rnd =  new scala.util.Random(42)
  val (cols, rows) = (10,  10)
  val bytes = Array.ofDim[Byte](cols * rows)
  rnd.nextBytes(bytes)
  val tile = ArrayTile.fromBytes(bytes, UByteCellType, cols, rows)
  println(tile.renderAscii())
}
