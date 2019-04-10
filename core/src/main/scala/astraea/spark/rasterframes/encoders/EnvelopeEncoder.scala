package astraea.spark.rasterframes.encoders

import org.locationtech.jts.geom.Envelope
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.objects.NewInstance
import org.apache.spark.sql.catalyst.expressions.{BoundReference, CreateNamedStruct, Literal}
import org.apache.spark.sql.rf.VersionShims.InvokeSafely
import org.apache.spark.sql.types._
import CatalystSerializer._
import scala.reflect.classTag

/**
 * Spark DataSet codec for JTS Envelope.
 *
 * @since 2/22/18
 */
object EnvelopeEncoder {

  val schema = schemaOf[Envelope]

  val dataType: DataType = ScalaReflection.dataTypeFor[Envelope]

  def apply(): ExpressionEncoder[Envelope] = {
    val inputObject = BoundReference(0, ObjectType(classOf[Envelope]), nullable = true)

    val invokers = schema.flatMap { f ⇒
      val getter = "get" + f.name.head.toUpper + f.name.tail
      Literal(f.name) :: InvokeSafely(inputObject, getter, DoubleType) :: Nil
    }

    val serializer = CreateNamedStruct(invokers)
    val deserializer = NewInstance(classOf[Envelope],
      (0 to 3).map(GetColumnByOrdinal(_, DoubleType)),
      dataType, false
    )

    new ExpressionEncoder[Envelope](schema, flat = false, serializer.flatten, deserializer, classTag[Envelope])
  }
}
