/*
 * Copyright (c) 2020 Astraea, Inc. All right reserved.
 */

package org.locationtech.rasterframes.datasource.slippy

import geotrellis.raster._
import geotrellis.raster.render.{ColorRamp, ColorRamps, Png}
import org.locationtech.rasterframes.stats.{CellHistogram, CellStatistics}

import scala.util.Try


private[slippy]
object RenderingProfiles {
  /** Base type for Rendering profiles. */
  trait Profile {
    /** Expected number of bands. */
    val expectedBands: Int
    /** Go from tile to PNG. */
    def render(tile: MultibandTile): Png
  }

  val greyscale = ColorRamps.greyscale(256)

  case class ColorMapProfile(cmap: ColorMap) extends Profile {
    val expectedBands: Int = 1
    override def render(tile: MultibandTile): Png = {
      require(tile.bandCount >= expectedBands, "Expected at least one band.")
      tile.band(0).renderPng(cmap)
    }
  }

  case class ColorRampProfile(ramp: ColorRamp, breaks: Int = 256) extends Profile {
    val expectedBands: Int = 1
    // Are there other ways to use the other bands?
    override def render(tile: MultibandTile): Png = {
      require(tile.bandCount >= expectedBands, s"Need at least 1 band")
      tile.band(0).renderPng(ramp)
    }
  }

  case class UniformColorRampProfile(ramp: ColorRamp, breaks: Int = 256) extends Profile {
    val expectedBands: Int = 1
    def toResolvedProfile(histo: CellHistogram): Profile =
      ColorMapProfile(ramp.toColorMap(histo.quantileBreaks(breaks)))
    override def render(tile: MultibandTile): Png = {
      // This hack around partially specifying a color
      // profile is likely anathema to many, but since this
      // class is package private and the semantics should only affect
      // sibling classes, I think it's an OK compromise for how.
      throw new IllegalStateException("Use requires call to `toColorMapProfile`")
    }
  }

  case class RGBColorProfile() extends Profile {
    val expectedBands: Int = 3
    override def render(tile: MultibandTile): Png = {
      val scaled = tile
        .mapBands((_, t) => Try(t.rescale(0, 255)).getOrElse(t))
      scaled.renderPng()
    }
  }

  case class UniformRGBColorProfile(private val stats: Seq[CellStatistics] = Seq.empty) extends Profile {
    /** Expected number of bands. */
    override val expectedBands: Int = 3

    def toResolvedProfile(red: CellStatistics, green: CellStatistics, blue: CellStatistics) = {
      UniformRGBColorProfile(Seq(red, green, blue))
    }

    /** Go from tile to PNG. */
    override def render(tile: MultibandTile): Png = {
      if (stats.isEmpty) {
        // See note in UniformColorRampProfile above
        throw new IllegalStateException("Use requires call to `toRGBColorProfile`")
      }
      else {
        // Hacky but fast multiband normalization.
        val scaled = tile.bands.zip(stats).map { case (t, s) =>
          val min = s.mean - 5 * s.stddev
          val max = s.mean + 5 * s.stddev
          t.normalize(min, max, 0, 255).convert(UByteConstantNoDataCellType)
        }

        // Couldn't get this to work
//        // If one of the channels is no-data, make all of them no-data
//        val mask = scaled.map(Defined.apply).reduce(And(_, _))
//        val masked = scaled.map(_.localMask(mask, 0, ubyteNODATA))
        MultibandTile(scaled).renderPng()
      }
    }
  }
}
