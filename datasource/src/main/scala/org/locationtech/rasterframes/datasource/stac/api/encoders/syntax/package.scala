package org.locationtech.rasterframes.datasource.stac.api.encoders

import io.circe.{Decoder, Json}
import cats.syntax.either._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

package object syntax {
  implicit class ExpressionEncoderOps[T](val t: T) extends AnyVal {
    def toInternalRow(implicit encoder: ExpressionEncoder[T]): InternalRow =
      encoder.createSerializer()(t)
  }

  implicit class InternalRowOps(val t: InternalRow) extends AnyVal {
    def as[T](implicit encoder: ExpressionEncoder[T]): T =
      encoder.createDeserializer()(t)
  }

  implicit class JsonOps(val json: Json) extends AnyVal {
    def asUnsafe[T: Decoder]: T = json.as[T].valueOr(throw _)
  }
}
