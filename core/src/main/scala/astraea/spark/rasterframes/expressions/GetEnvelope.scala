package astraea.spark.rasterframes.expressions
import astraea.spark.rasterframes.encoders.EnvelopeEncoder
import com.vividsolutions.jts.geom.Envelope
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.rf._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, TypedColumn}


/**
 * Extracts the bounding box (envelope) of arbitrary JTS Geometry.
 *
 * @since 2/22/18
 */
case class GetEnvelope(child: Expression) extends UnaryExpression
  with CodegenFallback with GeomDeserializerSupport  {

  override def nodeName: String = "envelope"

  override protected def nullSafeEval(input: Any): Any = {
    val geom = extractGeometry(child, input)
    val env = geom.getEnvelopeInternal
    InternalRow(env.getMinX, env.getMaxX, env.getMinY, env.getMaxY)
  }

  def dataType: DataType = EnvelopeEncoder.schema
}

object GetEnvelope  {
  import astraea.spark.rasterframes.encoders.StandardEncoders._
  def apply(col: Column): TypedColumn[Any, Envelope] =
    new GetEnvelope(col.expr).asColumn.as[Envelope]
}
