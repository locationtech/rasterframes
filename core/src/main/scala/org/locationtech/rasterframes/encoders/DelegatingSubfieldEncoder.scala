/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Astraea, Inc.
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
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.locationtech.rasterframes.encoders

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.{GetColumnByOrdinal, UnresolvedAttribute, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.objects.NewInstance
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.rf.VersionShims.InvokeSafely

import scala.reflect.runtime.universe.TypeTag

/**
 * Encoder builder for types composed of other fields with {{ExpressionEncoder}}s.
 *
 * @since 8/2/17
 */
object DelegatingSubfieldEncoder {
  def apply[T: TypeTag](
    fieldEncoders: (String, ExpressionEncoder[_])*): ExpressionEncoder[T] = {
    val schema = StructType(fieldEncoders.map {
      case (name, encoder) ⇒
        StructField(name, encoder.schema, false)
    })

    val parentType = ScalaReflection.dataTypeFor[T]

    val inputObject = BoundReference(0, parentType, nullable = false)
    val serializer = CreateNamedStruct(fieldEncoders.flatMap {
      case (name, encoder) ⇒
        val enc = encoder.serializer.map(_.transform {
          case r: BoundReference if r != inputObject ⇒
            InvokeSafely(inputObject, name, r.dataType)
        })
        Literal(name) :: CreateStruct(enc) :: Nil
    })

    val fieldDeserializers = fieldEncoders.map(_._2).zipWithIndex.map {
      case (enc, index) ⇒
        val input = GetColumnByOrdinal(index, enc.schema)
        val deserialized = enc.deserializer.transformUp {
          case UnresolvedAttribute(nameParts) ⇒
            UnresolvedExtractValue(input, Literal(nameParts.head))
          case GetColumnByOrdinal(ordinal, _) ⇒ GetStructField(input, ordinal)
        }
        If(IsNull(input), Literal.create(null, deserialized.dataType), deserialized)
    }

    val deserializer: Expression = NewInstance(runtimeClass[T], fieldDeserializers, parentType, propagateNull = false)

    ExpressionEncoder(schema, flat = false, serializer.flatten, deserializer, typeToClassTag[T])
  }
}
