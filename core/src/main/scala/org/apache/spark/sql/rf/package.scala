/*
 * Copyright 2017 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry, MultiAlias}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{CreateArray, Expression, Inline}
import org.apache.spark.sql.types.{DataType, StructType, UDTRegistration, UserDefinedType}

import scala.reflect.runtime.universe._

/**
 * Module providing support for Spark SQL UDTs.
 *
 * @since 3/30/17
 */
package object rf {
  implicit class CanBeColumn(expression: Expression) {
    @deprecated("Use `new Column(...)` instead", "0.8.0")
    def asColumn: Column = Column(expression)
  }

  implicit class WithTypeConformity(val left: DataType) extends AnyVal {
    def conformsTo(right: DataType): Boolean = right.acceptsType(left)
  }

  def register(sqlContext: SQLContext): Unit = {
    // Referencing the companion objects here is intended to have it's constructor called,
    // which is where the registration actually happens. The ordering matters!
    RasterSourceUDT
    TileUDT
    CrsUDT
  }

  def registry(sqlContext: SQLContext): FunctionRegistry = {
    sqlContext.sessionState.functionRegistry
  }

  def analyzer(sqlContext: SQLContext): Analyzer = {
    sqlContext.sessionState.analyzer
  }

  /** Lookup the registered Catalyst UDT for the given Scala type. */
  def udtOf[T >: Null: TypeTag]: UserDefinedType[T] =
    UDTRegistration.getUDTFor(typeTag[T].tpe.toString).map(_.getDeclaredConstructor().newInstance().asInstanceOf[UserDefinedType[T]])
      .getOrElse(throw new IllegalArgumentException(typeTag[T].tpe + " doesn't have a corresponding UDT"))

  /** Creates a Catalyst expression for flattening the fields in a struct into columns. */
  def projectStructExpression(dataType: StructType, input: Expression) =
    MultiAlias(Inline(CreateArray(Seq(input))), dataType.fields.map(_.name))

  implicit class WithPPrint[T](enc: ExpressionEncoder[T]) {
    def pprint(): Unit = {
      println(enc.getClass.getSimpleName + "{")
      println("\tschema=" + enc.schema)
      println("\tserializers=" + enc.serializer)
      println("\tnamedExpressions=" + enc.namedExpressions)
      println("\tdeserializer=" + enc.deserializer)
      println("}")
    }
  }
}
