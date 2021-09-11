package org.locationtech.rasterframes.encoders

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.reflect.runtime.universe.TypeTag

package object syntax {
  implicit class CachedExpressionOps[T](val self: T) extends AnyVal {
    def toInternalRow(implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): InternalRow = {
      val toRow = SerializersCache.serializer[T]
      toRow(self).copy()
    }

    def toRow(implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): Row = {
      val toRow = SerializersCache.rowSerialize[T]
      toRow(self)
    }
  }

  implicit class CachedExpressionRowOps(val self: Row) extends AnyVal {
    def as[T](implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): T = {
      val fromRow = SerializersCache.rowDeserialize[T]
      fromRow(self)
    }
  }

  implicit class CachedInternalRowOps(val self: InternalRow) extends AnyVal {
    def as[T](implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): T = {
      val fromRow = SerializersCache.deserializer[T]
      fromRow(self)
    }
  }
}
