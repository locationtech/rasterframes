package astraea.spark.rasterframes.expressions
import astraea.spark.rasterframes.encoders.EnvelopeEncoder
import com.vividsolutions.jts.geom.{Envelope, Geometry}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.jts.AbstractGeometryUDT
import org.apache.spark.sql.rf._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, TypedColumn}

/**
 * Extracts the bounding box (envelope) of arbitrary JTS Geometry.
 *
 * @since 2/22/18
 */
@deprecated("Replace usages of this with GeometryToBounds", "11/4/2018")
case class GetEnvelope(child: Expression) extends UnaryExpression with CodegenFallback {

  override def nodeName: String = "envelope"
  def extractGeometry(expr: Expression, input: Any): Geometry = {
    input match {
      case g: Geometry => g
      case r: InternalRow =>
        expr.dataType match {
          case udt: AbstractGeometryUDT[_] => udt.deserialize(r)
        }
    }
  }

  override protected def nullSafeEval(input: Any): Any = {
    val geom = extractGeometry(child, input)
    val env = geom.getEnvelopeInternal
    InternalRow(env.getMinX, env.getMaxX, env.getMinY, env.getMaxY)
  }

  def dataType: DataType = EnvelopeEncoder.schema
}

object GetEnvelope {
  import astraea.spark.rasterframes.encoders.StandardEncoders._
  def apply(col: Column): TypedColumn[Any, Envelope] =
    new GetEnvelope(col.expr).asColumn.as[Envelope]
}
