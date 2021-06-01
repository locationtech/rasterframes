package org.locationtech.rasterframes.encoders

import org.apache.spark.sql.catalyst.DeserializerBuildHelper.{addToPath, deserializerForWithNullSafetyAndUpcast, expressionWithNullSafety}
import org.apache.spark.sql.catalyst.ScalaReflection.{Schema, cleanUpReflectionObjects, dataTypeFor, encodeFieldNameToIdentifier, getClassFromType, getClassNameFromType, schemaFor}
import org.apache.spark.sql.catalyst.SerializerBuildHelper.createSerializerForObject
import org.apache.spark.sql.catalyst.{WalkedTypePath, expressions}
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, NewInstance}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, IsNull, KnownNotNull}
import org.apache.spark.sql.types.ObjectType
import org.locationtech.rasterframes.encoders.Person.{deserializerFor, deserializerForPerson, serializerFor, serializerForPerson}
import org.apache.spark.sql.catalyst.ScalaReflection.universe.{AnnotatedType, Type, typeOf, typeTag}
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal

import javax.lang.model.SourceVersion

case class School(teacher: Person, student: Person)

object School {
  def encoder: Expression = {
    val tpe = typeOf[School]
    val clsName = getClassNameFromType(tpe)
    val walkedTypePath = new WalkedTypePath().recordRoot(clsName)

    // The input object to `ExpressionEncoder` is located at first column of an row.
    val isPrimitive = tpe.typeSymbol.asClass.isPrimitive
    val inputObject = BoundReference(0, dataTypeFor(typeTag[School]), nullable = !isPrimitive)

    serializerForSchool(inputObject, tpe, walkedTypePath)
  }

  private def serializerForSchool(
    inputObject: Expression,
    tpe: `Type`,
    walkedTypePath: WalkedTypePath,
    seenTypeSet: Set[`Type`] = Set.empty
  ): Expression = cleanUpReflectionObjects {
    val params = List(("teacher", typeOf[Person], ObjectType(classOf[Person])), ("student", typeOf[Person], ObjectType(classOf[Person])))

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

      (fieldName, Person.serializerForPerson(fieldValue, fieldType, newPath, seenTypeSet + tpe))
    }
    createSerializerForObject(inputObject, fields)
  }

  def decoder: Expression = {
    val tpe = typeOf[School]
    val clsName = getClassNameFromType(tpe)
    val walkedTypePath = new WalkedTypePath().recordRoot(clsName)
    val Schema(dataType, nullable) = schemaFor(tpe)

    // Assumes we are deserializing the first column of a row.
    deserializerForWithNullSafetyAndUpcast(GetColumnByOrdinal(0, dataType), dataType,
      nullable = nullable, walkedTypePath,
      (casted, typePath) => deserializerForSchool(tpe, casted, typePath))
  }

  def deserializerForSchool(
    tpe: `Type`,
    path: Expression,
    walkedTypePath: WalkedTypePath
  ): Expression = cleanUpReflectionObjects {
    val params: Seq[(String, Type)] = List(("teacher", typeOf[Person]), ("student", typeOf[Person]))

    val cls = getClassFromType(tpe)

    val arguments: Seq[Expression] = params.zipWithIndex.map { case ((fieldName, fieldType), i) =>
      val Schema(dataType, nullable) = schemaFor(fieldType)
      val clsName = getClassNameFromType(fieldType)
      val newTypePath = walkedTypePath.recordField(clsName, fieldName)

      // For tuples, we based grab the inner fields by ordinal instead of name.
      val newPath =
        deserializerForPerson(
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

}
