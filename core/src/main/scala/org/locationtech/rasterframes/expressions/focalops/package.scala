package org.locationtech.rasterframes.expressions

import geotrellis.raster.Tile
import org.locationtech.rasterframes.ref.RasterRef
import org.locationtech.rasterframes.tiles.ProjectedRasterTile

package object focalops extends Serializable {
  private [focalops] def extractBufferTile(tile: Tile): Tile = tile match {
    // if it is RasterRef, we want the BufferTile
    case ref: RasterRef => ref.realizedTile
    // if it is a ProjectedRasterTile, can we flatten it?
    case prt: ProjectedRasterTile => prt.tile match {
      // if it is RasterRef, we can get what's inside
      case rr: RasterRef => rr.realizedTile
      // otherwise it is some tile
      case _             => prt.tile
    }
    case _ => tile
  }
}
