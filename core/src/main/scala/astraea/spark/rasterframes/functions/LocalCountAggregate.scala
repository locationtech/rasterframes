package astraea.spark.rasterframes.functions

import geotrellis.raster.mapalgebra.local.{Add, Defined, Undefined}
import geotrellis.raster.{IntConstantNoDataCellType, Tile}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.rf.TileUDT
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
 * Catalyst aggregate function that counts `NoData` values in a cell-wise fashion.
 *
 * @param isData true if count should be of non-NoData values, false for NoData values.
 * @since 8/11/17
 */
class LocalCountAggregate(isData: Boolean) extends UserDefinedAggregateFunction {

  private val incCount =
    if (isData) safeBinaryOp((t1: Tile, t2: Tile) ⇒ Add(t1, Defined(t2)))
    else safeBinaryOp((t1: Tile, t2: Tile) ⇒ Add(t1, Undefined(t2)))

  private val add = safeBinaryOp(Add.apply(_: Tile, _: Tile))

  private val TileType = new TileUDT()

  override def dataType: DataType = TileType

  override def inputSchema: StructType = StructType(StructField("value", TileType) :: Nil)

  override def bufferSchema: StructType = inputSchema

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit =
    buffer(0) = null

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val right = input.getAs[Tile](0)
    if (right != null) {
      if (buffer(0) == null) {
        buffer(0) = (
          if (isData) Defined(right) else Undefined(right)
          ).convert(IntConstantNoDataCellType)
      } else {
        val left = buffer.getAs[Tile](0)
        buffer(0) = incCount(left, right)
      }
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = add(buffer1.getAs[Tile](0), buffer2.getAs[Tile](0))
  }

  override def evaluate(buffer: Row): Tile = buffer.getAs[Tile](0)
}
