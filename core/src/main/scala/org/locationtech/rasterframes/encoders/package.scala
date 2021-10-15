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

package org.locationtech.rasterframes

import org.locationtech.rasterframes.encoders.syntax._

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.Literal

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import frameless.TypedEncoder
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.rf.WithTypeConformity

/**
 * Module utilities
 *
 * @since 9/25/17
 */
package object encoders {
  /** High priority specific product encoder derivation. Without it, the default spark would be used. */
  implicit def productTypedToExpressionEncoder[T <: Product: TypedEncoder]: ExpressionEncoder[T] = TypedEncoders.typedExpressionEncoder

  implicit class WithTypeConformityToEncoder(val left: DataType) extends AnyVal {
    def conformsToSchema(schema: StructType): Boolean =
      WithTypeConformity(left).conformsTo(schema)

    def conformsToDataType(dataType: DataType): Boolean =
      WithTypeConformity(left).conformsTo(dataType)
  }

  private[rasterframes] def runtimeClass[T: TypeTag]: Class[T] =
    typeTag[T].mirror.runtimeClass(typeTag[T].tpe).asInstanceOf[Class[T]]

  private[rasterframes] def typeToClassTag[T: TypeTag]: ClassTag[T] = {
    ClassTag[T](typeTag[T].mirror.runtimeClass(typeTag[T].tpe))
  }

  /** Constructs a catalyst literal expression from anything with a serializer.
   * Using this serializer avoids using lit() function wich will defer to ScalaReflection to derive encoder.
   * Therefore, this should be used when literal value can not be handled by Spark ScalaReflection.
   */
  def SerializedLiteral[T >: Null](t: T)(implicit tag: TypeTag[T], enc: ExpressionEncoder[T]): Literal = {
    val schema = enc.schema match {
      case s if s.conformsTo(tileUDT.sqlType) => tileUDT
      case s if s.conformsTo(rasterSourceUDT.sqlType) => rasterSourceUDT
      case s => s
    }
    // we need to convert to Literal right here because otherwise ScalaReflection takes over
    val ir = t.toInternalRow
    Literal.create(ir, schema)
  }

  /**
   * Constructs a Dataframe literal column from anything with a serializer.
   * TODO: review its usage.
   */
  def serialized_literal[T >: Null: ExpressionEncoder: TypeTag](t: T): Column =
    new Column(SerializedLiteral(t))

}
