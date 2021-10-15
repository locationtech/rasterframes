package org.locationtech.rasterframes.encoders

import frameless.{RecordEncoderField, TypedEncoder}
import org.apache.spark.sql.FramelessInternals
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, InvokeLike, NewInstance, StaticInvoke}
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, Expression, GetStructField, If, IsNull, Literal}
import org.apache.spark.sql.types.{DataType, Metadata, StructField, StructType}

import scala.reflect.{ClassTag, classTag}

/** Can be useful for non Scala types and for complicated case classes with implicits in the constructor. */
object ManualTypedEncoder {
  /** Invokes apply from the companion object. */
  def staticInvoke[T: ClassTag](
    fields: List[RecordEncoderField],
    fieldNameModify: String => String = identity,
    isNullable: Boolean = true
  ): TypedEncoder[T] = apply[T](fields, { (classTag, newArgs, jvmRepr) => StaticInvoke(classTag.runtimeClass, jvmRepr, "apply", newArgs, propagateNull = true, returnNullable = false) }, fieldNameModify, isNullable)

  /** Invokes object constructor. */
  def newInstance[T: ClassTag](
    fields: List[RecordEncoderField],
    fieldNameModify: String => String = identity,
    isNullable: Boolean = true
  ): TypedEncoder[T] = apply[T](fields, { (classTag, newArgs, jvmRepr) => NewInstance(classTag.runtimeClass, newArgs, jvmRepr, propagateNull = true) }, fieldNameModify, isNullable)

  def apply[T: ClassTag](
    fields: List[RecordEncoderField],
    newInstanceExpression: (ClassTag[T], Seq[Expression], DataType) => InvokeLike,
    fieldNameModify: String => String = identity,
    isNullable: Boolean = true
  ): TypedEncoder[T] = make[T](fields, newInstanceExpression, fieldNameModify, isNullable, classTag[T])

  private def make[T](
    // the catalyst struct
    fields: List[RecordEncoderField],
    // newInstanceExpression for the fromCatalyst function
    newInstanceExpression: (ClassTag[T], Seq[Expression], DataType) => InvokeLike,
    // allows to convert the field name into the field name getter
    fieldNameModify: String => String,
    // is the codec nullable
    isNullable: Boolean,
    // ClassTag is required for the TypedEncoder constructor
    // it is passed explicitly to disambiguate ClassTag passed implicitly as a function argument
    // and the one from the TypedEncoder constructor
    ct: ClassTag[T]
  ): TypedEncoder[T] = new TypedEncoder[T]()(ct) {
    def nullable: Boolean = isNullable

    def jvmRepr: DataType = FramelessInternals.objectTypeFor[T]

    def catalystRepr: DataType = {
      val structFields = fields.map { field =>
        StructField(
          name = field.name,
          dataType = field.encoder.catalystRepr,
          nullable = field.encoder.nullable,
          metadata = Metadata.empty
        )
      }

      StructType(structFields)
    }

    def fromCatalyst(path: Expression): Expression = {
      val newArgs: Seq[Expression] = fields.map { field =>
        field.encoder.fromCatalyst( GetStructField(path, field.ordinal, Some(field.name)) )
      }
      val newExpr = newInstanceExpression(classTag, newArgs, jvmRepr)

      val nullExpr = Literal.create(null, jvmRepr)
      If(IsNull(path), nullExpr, newExpr)
    }

    def toCatalyst(path: Expression): Expression = {
      val nameExprs = fields.map { field => Literal(field.name) }

      val valueExprs: Seq[Expression] = fields.map { field =>
        val fieldPath = Invoke(path, fieldNameModify(field.name), field.encoder.jvmRepr, Nil)
        field.encoder.toCatalyst(fieldPath)
      }

      // the way exprs are encoded in CreateNamedStruct
      val exprs = nameExprs.zip(valueExprs).flatMap { case (nameExpr, valueExpr) => nameExpr :: valueExpr :: Nil }

      val createExpr = CreateNamedStruct(exprs)
      val nullExpr = Literal.create(null, createExpr.dataType)
      If(IsNull(path), nullExpr, createExpr)
    }
  }
}
