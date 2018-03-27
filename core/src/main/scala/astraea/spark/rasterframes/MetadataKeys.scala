package astraea.spark.rasterframes

/**
 *
 * @since 2/19/18
 */
trait MetadataKeys {
  /** Key under which ContextRDD metadata is stored. */
  private[rasterframes] val CONTEXT_METADATA_KEY = "_context"

  /** Key under which RasterFrame role a column plays. */
  private[rasterframes] val SPATIAL_ROLE_KEY = "_stRole"
}
