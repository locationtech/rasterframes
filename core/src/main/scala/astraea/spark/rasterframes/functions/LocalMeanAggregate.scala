package astraea.spark.rasterframes.functions

import org.apache.spark.sql.Row
import org.apache.spark.sql.rf.TileUDT
import org.apache.spark.sql.types.DataType

/**
 * Aggregation function that only returns the average. Depends on
 * [[LocalStatsAggregate]] for computation and just
 * selects the mean result tile.
 *
 * @since 8/11/17
 */
class LocalMeanAggregate extends LocalStatsAggregate {
  override def dataType: DataType = new TileUDT()
  override def evaluate(buffer: Row): Any = {
    val superRow = super.evaluate(buffer).asInstanceOf[Row]
    if (superRow != null) superRow.get(3) else null
  }
}
