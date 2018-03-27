package astraea.spark.rasterframes

import java.sql.Timestamp

import geotrellis.raster.{Tile, TileFeature}
import geotrellis.spark.{SpatialKey, TemporalKey}
import org.apache.spark.sql.functions.col
import com.vividsolutions.jts.geom.{Point ⇒ jtsPoint, Polygon ⇒ jtsPolygon}
import astraea.spark.rasterframes.encoders.SparkDefaultEncoders._
import astraea.spark.rasterframes.encoders.StandardEncoders

/**
 * Constants identifying column in most RasterFrames.
 *
 * @since 2/19/18
 */
trait StandardColumns extends StandardEncoders {
  /** Default RasterFrame spatial column name. */
  val SPATIAL_KEY_COLUMN = col("spatial_key").as[SpatialKey]

  /** Default RasterFrame temporal column name. */
  val TEMPORAL_KEY_COLUMN = col("temporal_key").as[TemporalKey]

  /** Default RasterFrame timestamp column name */
  val TIMESTAMP_COLUMN = col("timestamp").as[Timestamp]

  /** Default RasterFrame column name for an tile bounds value. */
  // This is a `def` because `PolygonUDT` needs to be initialized first.
  def BOUNDS_COLUMN = col("bounds").as[jtsPolygon]

  /** Default RasterFrame column name for the center coordinates of the tile's bounds. */
  // This is a `def` because `PointUDT` needs to be initialized first.
  def CENTER_COLUMN = col("center").as[jtsPoint]

  /** Default RasterFrame column name for an added spatial index. */
  val SPATIAL_INDEX_COLUMN = col("spatial_index").as[Long]

  /** Default RasterFrame tile column name. */
  // This is a `def` because `TileUDT` needs to be initialized first.
  def TILE_COLUMN = col("tile").as[Tile]

  /** Default RasterFrame [[TileFeature.data]] column name. */
  val TILE_FEATURE_DATA_COLUMN = col("tile_data")

  /** Default GeoTiff tags column. */
  val METADATA_COLUMN = col("metadata").as[Map[String, String]]

  /** Default column index column for the cells of exploded tiles. */
  val COLUMN_INDEX_COLUMN = col("column_index").as[Int]

  /** Default teil column index column for the cells of exploded tiles. */
  val ROW_INDEX_COLUMN = col("row_index").as[Int]
}

object StandardColumns extends StandardColumns
