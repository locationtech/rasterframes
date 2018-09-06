package astraea.spark.rasterframes.expressions
import astraea.spark.rasterframes.encoders.EnvelopeEncoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.types._


/**
 * Extracts the bounding box (envelope) of arbitrary JTS Geometry.
 *
 * @since 2/22/18
 */
case class EnvelopeExpression(child: Expression) extends UnaryExpression
  with CodegenFallback with GeomDeserializerSupport  {

  override def nodeName: String = "envelope"

  override protected def nullSafeEval(input: Any): Any = {
    val geom = extractGeometry(child, input)
    val env = geom.getEnvelopeInternal
    InternalRow(env.getMinX, env.getMaxX, env.getMinY, env.getMaxY)
  }

  def dataType: DataType = EnvelopeEncoder.schema
}
