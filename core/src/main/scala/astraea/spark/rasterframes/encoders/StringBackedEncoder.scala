/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     [http://www.apache.org/licenses/LICENSE-2.0]
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package astraea.spark.rasterframes.encoders

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.rf.VersionShims.InvokeSafely

import scala.reflect.runtime.universe._

/**
 * Generalized operations for creating an encoder when the type can be represented as a Catalyst string.
 *
 * @since 1/16/18
 */
object StringBackedEncoder {
  def apply[T: TypeTag](
    fieldName: String,
    toStringMethod: String,
    fromStringStatic: (Class[_], String)): ExpressionEncoder[T] = {

    val sparkType = ScalaReflection.dataTypeFor[T]
    val schema = StructType(Seq(StructField(fieldName, StringType, false)))
    val inputObject = BoundReference(0, sparkType, nullable = false)

    val intermediateType = ObjectType(classOf[String])
    val serializer: Expression =
      StaticInvoke(
        classOf[UTF8String],
        StringType,
        "fromString",
        InvokeSafely(inputObject, toStringMethod, intermediateType) :: Nil
      )

    val inputRow = GetColumnByOrdinal(0, schema)
    val deserializer: Expression =
      StaticInvoke(
        fromStringStatic._1,
        sparkType,
        fromStringStatic._2,
        InvokeSafely(inputRow, "toString", intermediateType) :: Nil
      )

    ExpressionEncoder[T](schema, flat = false, Seq(serializer), deserializer, typeToClassTag[T])
  }
}
