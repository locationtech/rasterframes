package astraea.spark.rasterframes.expressions

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.jts.AbstractGeometryUDT

/**
 * Support for deserializing JTS geometry inside expressions.
 *
 * @since 2/22/18
 */
trait GeomDeserializerSupport {
  def extractGeometry(expr: Expression, input: Any): Geometry = {
    input match {
      case g: Geometry ⇒ g
      case r: InternalRow ⇒
        expr.dataType match {
          case udt: AbstractGeometryUDT[_] ⇒ udt.deserialize(r)
        }
    }
  }
}
