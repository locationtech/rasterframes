package org.locationtech.rasterframes.encoders

import org.apache.spark.sql.catalyst.DeserializerBuildHelper.{addToPath, createDeserializerForInstant, createDeserializerForLocalDate, createDeserializerForSqlDate, createDeserializerForSqlTimestamp, createDeserializerForString, createDeserializerForTypesSupportValueOf, deserializerForWithNullSafetyAndUpcast, expressionWithNullSafety}
import org.apache.spark.sql.catalyst.ScalaReflection.{Schema, arrayClassFor, cleanUpReflectionObjects, dataTypeFor, encodeFieldNameToIdentifier, getClassFromType, getClassNameFromType, getConstructorParameters, isSubtype, localTypeOf, schemaFor}
import org.apache.spark.sql.catalyst.SerializerBuildHelper.{createSerializerForBoolean, createSerializerForByte, createSerializerForDouble, createSerializerForFloat, createSerializerForInteger, createSerializerForJavaBigDecimal, createSerializerForJavaBigInteger, createSerializerForJavaInstant, createSerializerForJavaLocalDate, createSerializerForLong, createSerializerForObject, createSerializerForScalaBigDecimal, createSerializerForScalaBigInt, createSerializerForShort, createSerializerForSqlDate, createSerializerForSqlTimestamp, createSerializerForString}
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.{WalkedTypePath, expressions}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, IsNull, KnownNotNull}
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, NewInstance}

import javax.lang.model.SourceVersion
import org.apache.spark.sql.catalyst.ScalaReflection.universe.{AnnotatedType, Type, typeOf, typeTag}
import org.apache.spark.unsafe.types.CalendarInterval

case class Person(name: String, age: Integer)

object Person {
  private def baseType(tpe: `Type`): `Type` = {
    tpe.dealias match {
      case annotatedType: AnnotatedType => annotatedType.underlying
      case other => other
    }
  }

  object ScalaSubtypeLock

  private def isSubtype(tpe1: `Type`, tpe2: `Type`): Boolean = {
    ScalaSubtypeLock.synchronized {
      tpe1 <:< tpe2
    }
  }

  import org.apache.spark.sql.types._



  def encoder: Expression = {
    val tpe = typeOf[Person]
    val clsName = getClassNameFromType(tpe)
    val walkedTypePath = new WalkedTypePath().recordRoot(clsName)

    // The input object to `ExpressionEncoder` is located at first column of an row.
    val isPrimitive = tpe.typeSymbol.asClass.isPrimitive
    val inputObject = BoundReference(0, dataTypeFor(typeTag[Person]), nullable = !isPrimitive)

    serializerForPerson(inputObject, tpe, walkedTypePath)
  }


  def serializerForPerson(
    inputObject: Expression,
    tpe: `Type`,
    walkedTypePath: WalkedTypePath,
    seenTypeSet: Set[`Type`] = Set.empty): Expression = cleanUpReflectionObjects {

    val params = List(("name", typeOf[String], ObjectType(classOf[String])), ("age", typeOf[Integer], ObjectType(classOf[Integer])))

    val fields = params.map { case (fieldName, fieldType, dt) =>
      if (SourceVersion.isKeyword(fieldName) ||
        !SourceVersion.isIdentifier(encodeFieldNameToIdentifier(fieldName))) {
        throw new UnsupportedOperationException(s"`$fieldName` is not a valid identifier of " +
          "Java and cannot be used as field name\n" + walkedTypePath)
      }

      // SPARK-26730 inputObject won't be null with If's guard below. And KnownNotNul
      // is necessary here. Because for a nullable nested inputObject with struct data
      // type, e.g. StructType(IntegerType, StringType), it will return nullable=true
      // for IntegerType without KnownNotNull. And that's what we do not expect to.
      val fieldValue = Invoke(KnownNotNull(inputObject), fieldName, dt,
        returnNullable = !fieldType.typeSymbol.asClass.isPrimitive)
      val clsName = getClassNameFromType(fieldType)
      val newPath = walkedTypePath.recordField(clsName, fieldName)

      (fieldName, serializerFor(fieldValue, fieldType, newPath, seenTypeSet + tpe))
      // TODO: serializerFor should be for primitive or for TC
      // I guess TC just returns the serializer Expression, each instance just gets the type per field
    }
    createSerializerForObject(inputObject, fields)
  }


  private def serializerFor(
    inputObject: Expression,
    tpe: `Type`,
    walkedTypePath: WalkedTypePath,
    seenTypeSet: Set[`Type`] = Set.empty): Expression = cleanUpReflectionObjects {

    baseType(tpe) match {
      case _ if !inputObject.dataType.isInstanceOf[ObjectType] =>
        inputObject

      // Since List[_] also belongs to localTypeOf[Product], we put this case before
      // "case t if definedByConstructorParams(t)" to make sure it will match to the
      // case "localTypeOf[Seq[_]]"

      case t if isSubtype(t, localTypeOf[String]) =>
        createSerializerForString(inputObject)

      case t if isSubtype(t, localTypeOf[java.time.Instant]) =>
        createSerializerForJavaInstant(inputObject)

      case t if isSubtype(t, localTypeOf[java.sql.Timestamp]) =>
        createSerializerForSqlTimestamp(inputObject)

      case t if isSubtype(t, localTypeOf[java.time.LocalDate]) =>
        createSerializerForJavaLocalDate(inputObject)

      case t if isSubtype(t, localTypeOf[java.sql.Date]) => createSerializerForSqlDate(inputObject)

      case t if isSubtype(t, localTypeOf[BigDecimal]) =>
        createSerializerForScalaBigDecimal(inputObject)

      case t if isSubtype(t, localTypeOf[java.math.BigDecimal]) =>
        createSerializerForJavaBigDecimal(inputObject)

      case t if isSubtype(t, localTypeOf[java.math.BigInteger]) =>
        createSerializerForJavaBigInteger(inputObject)

      case t if isSubtype(t, localTypeOf[scala.math.BigInt]) =>
        createSerializerForScalaBigInt(inputObject)

      case t if isSubtype(t, localTypeOf[java.lang.Integer]) =>
        createSerializerForInteger(inputObject)
      case t if isSubtype(t, localTypeOf[java.lang.Long]) => createSerializerForLong(inputObject)
      case t if isSubtype(t, localTypeOf[java.lang.Double]) =>
        createSerializerForDouble(inputObject)
      case t if isSubtype(t, localTypeOf[java.lang.Float]) => createSerializerForFloat(inputObject)
      case t if isSubtype(t, localTypeOf[java.lang.Short]) => createSerializerForShort(inputObject)
      case t if isSubtype(t, localTypeOf[java.lang.Byte]) => createSerializerForByte(inputObject)
      case t if isSubtype(t, localTypeOf[java.lang.Boolean]) =>
        createSerializerForBoolean(inputObject)
    }
  }



  def decoder: Expression = {
    val tpe = typeOf[Person]
    val clsName = getClassNameFromType(tpe)
    val walkedTypePath = new WalkedTypePath().recordRoot(clsName)
    val Schema(dataType, nullable) = schemaFor(tpe)

    // Assumes we are deserializing the first column of a row.
    deserializerForWithNullSafetyAndUpcast(GetColumnByOrdinal(0, dataType), dataType,
      nullable = nullable, walkedTypePath,
      (casted, typePath) => deserializerForPerson(tpe, casted, typePath))
  }

  def deserializerForPerson(
    tpe: `Type`,
    path: Expression,
    walkedTypePath: WalkedTypePath): Expression = cleanUpReflectionObjects {
    //val params = getConstructorParameters(t)
    val params: Seq[(String, Type)] = List(("name", typeOf[String]), ("age", typeOf[Integer]))

    val cls = getClassFromType(tpe)

    val arguments: Seq[Expression] = params.zipWithIndex.map { case ((fieldName, fieldType), i) =>
      val Schema(dataType, nullable) = schemaFor(fieldType)
      val clsName = getClassNameFromType(fieldType)
      val newTypePath = walkedTypePath.recordField(clsName, fieldName)

      // For tuples, we based grab the inner fields by ordinal instead of name.
      val newPath =
        deserializerFor(
          fieldType,
          addToPath(path, fieldName, dataType, newTypePath),
          newTypePath)

      expressionWithNullSafety(
        newPath,
        nullable = nullable,
        newTypePath)
    }

    val newInstance = NewInstance(cls, arguments, ObjectType(cls), propagateNull = false)

    expressions.If(
      IsNull(path),
      expressions.Literal.create(null, ObjectType(cls)),
      newInstance
    )
  }

  private def deserializerFor(
    tpe: `Type`,
    path: Expression,
    walkedTypePath: WalkedTypePath): Expression = cleanUpReflectionObjects {
    baseType(tpe) match {
      case t if isSubtype(t, localTypeOf[java.lang.Integer]) =>
        createDeserializerForTypesSupportValueOf(path,
          classOf[java.lang.Integer])

      case t if isSubtype(t, localTypeOf[java.lang.Long]) =>
        createDeserializerForTypesSupportValueOf(path,
          classOf[java.lang.Long])

      case t if isSubtype(t, localTypeOf[java.lang.Double]) =>
        createDeserializerForTypesSupportValueOf(path,
          classOf[java.lang.Double])

      case t if isSubtype(t, localTypeOf[java.lang.Float]) =>
        createDeserializerForTypesSupportValueOf(path,
          classOf[java.lang.Float])

      case t if isSubtype(t, localTypeOf[java.lang.Short]) =>
        createDeserializerForTypesSupportValueOf(path,
          classOf[java.lang.Short])

      case t if isSubtype(t, localTypeOf[java.lang.Byte]) =>
        createDeserializerForTypesSupportValueOf(path,
          classOf[java.lang.Byte])

      case t if isSubtype(t, localTypeOf[java.lang.Boolean]) =>
        createDeserializerForTypesSupportValueOf(path,
          classOf[java.lang.Boolean])

      case t if isSubtype(t, localTypeOf[java.time.LocalDate]) =>
        createDeserializerForLocalDate(path)

      case t if isSubtype(t, localTypeOf[java.sql.Date]) =>
        createDeserializerForSqlDate(path)

      case t if isSubtype(t, localTypeOf[java.time.Instant]) =>
        createDeserializerForInstant(path)

      case t if isSubtype(t, localTypeOf[java.sql.Timestamp]) =>
        createDeserializerForSqlTimestamp(path)

      case t if isSubtype(t, localTypeOf[java.lang.String]) =>
        createDeserializerForString(path, returnNullable = false)

    }
  }


}

