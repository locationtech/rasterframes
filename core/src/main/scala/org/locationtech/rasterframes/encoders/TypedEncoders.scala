package org.locationtech.rasterframes.encoders

import frameless._
import geotrellis.raster.CellType
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.rf.CrsUDT
import org.apache.spark.sql.rf.TileUDT

trait TypedEncoders {
  def typedExpressionEncoder[T: TypedEncoder]: ExpressionEncoder[T] =
    TypedExpressionEncoder[T].asInstanceOf[ExpressionEncoder[T]]

  implicit val crsUdt = new CrsUDT
  implicit val tileUdt = new TileUDT
  implicit def cellTypeInjection: Injection[CellType, String] = Injection(_.toString, CellType.fromName)
  implicit def cellTypeTypedEncoder: TypedEncoder[CellType] = TypedEncoder.usingInjection[CellType, String]
}

object TypedEncoders extends TypedEncoders