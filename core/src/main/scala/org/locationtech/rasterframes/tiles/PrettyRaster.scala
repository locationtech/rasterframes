package org.locationtech.rasterframes.tiles

import geotrellis.raster.{Tile}
import org.locationtech.rasterframes.model.TileContext

/**
 * TODO: Rename
 *
 * This is a replacement for ProjectedRasterTile that can be serialized using normal routes.
 * The plan is to start using PrettyRaster instead of ProjectedRasterTile in all contexts and then rename it.
 * @param tile_context
 * @param tile
 */
case class PrettyRaster (tile_context: TileContext, tile: Tile)
