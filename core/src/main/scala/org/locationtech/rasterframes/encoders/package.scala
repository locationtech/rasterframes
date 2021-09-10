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

import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.Literal

import scala.collection.concurrent.TrieMap
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{Literal => _, _}
import frameless.TypedEncoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.rf.WithTypeConformity

/**
 * Module utilities
 *
 * @since 9/25/17
 */
package object encoders { self =>
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
    val ser = cachedSerializer[T]
    val schema = enc.schema match {
      case s if s.conformsTo(tileUDT.sqlType) => tileUDT
      case s if s.conformsTo(rasterSourceUDT.sqlType) => rasterSourceUDT
      case s => s
    }
    // we need to convert to Literal right here because otherwise ScalaReflection takes over
    val ir = ser(t).copy()
    Literal.create(ir, schema)
  }

  /**
   * Constructs a Dataframe literal column from anything with a serializer.
   * TODO: review its usage.
   */
  def serialized_literal[T >: Null: ExpressionEncoder: TypeTag](t: T): Column =
    new Column(SerializedLiteral(t))

  case class TDeserializer[T](underlying: ExpressionEncoder.Deserializer[T]) extends AnyVal {
    def apply(i: InternalRow): T = self.synchronized(underlying.apply(i))
  }

  private val cacheSerializer: TrieMap[TypeTag[_], ExpressionEncoder.Serializer[_]] = TrieMap.empty
  private val cacheRowSerializer: TrieMap[TypeTag[_], ExpressionEncoder.Serializer[Row]] = TrieMap.empty
  private val cacheDeserializer: TrieMap[TypeTag[_], TDeserializer[_]] = TrieMap.empty
  private val cacheRowDeserializer: TrieMap[TypeTag[_], TDeserializer[Row]] = TrieMap.empty

  /** Serializer is threadsafe.*/
  def cachedSerializer[T](implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): ExpressionEncoder.Serializer[T] =
    cacheSerializer
      .getOrElseUpdate(tag, encoder.createSerializer())
      .asInstanceOf[ExpressionEncoder.Serializer[T]]

  def cachedRowSerializer[T](implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): ExpressionEncoder.Serializer[Row] =
    cacheRowSerializer.getOrElseUpdate(tag, RowEncoder(encoder.schema).createSerializer())

  /** Deserializer is not thread safe, and expensive to derive.
   * Per partition instance would give us no performance regressions,
   * however would require a significant DynamicExtractors refactor. */
  def cachedDeserializer[T](implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): TDeserializer[T] =
    cacheDeserializer
      .getOrElseUpdate(tag, TDeserializer(encoder.resolveAndBind().createDeserializer()))
      .asInstanceOf[TDeserializer[T]]

  def cachedRowDeserializer[T](implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): TDeserializer[Row] =
    cacheRowDeserializer.getOrElseUpdate(tag, TDeserializer(RowEncoder(encoder.schema).resolveAndBind().createDeserializer()))

  def cachedRowDeserialize[T](implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): Row => T = { row =>
    cachedDeserializer[T](tag, encoder)(cachedRowSerializer[T](tag, encoder)(row))
  }

  def cachedRowSerialize[T](implicit tag: TypeTag[T], encoder: ExpressionEncoder[T]): T => Row = { t =>
    cachedRowDeserializer[T](tag, encoder)(cachedSerializer[T](tag, encoder)(t))
  }
}
