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
 */

package astraea.spark.rasterframes

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Literal

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{Literal ⇒ _, _}



/**
 * Module utilities
 *
 * @since 9/25/17
 */
package object encoders {
  private[rasterframes] def runtimeClass[T: TypeTag]: Class[T] =
    typeTag[T].mirror.runtimeClass(typeTag[T].tpe).asInstanceOf[Class[T]]

  private[rasterframes] def typeToClassTag[T: TypeTag]: ClassTag[T] = {
    ClassTag[T](typeTag[T].mirror.runtimeClass(typeTag[T].tpe))
  }

  /** Constructs a Dataframe literal from anything with a serializer. */
  def serialized_literal[T >: Null: CatalystSerializer](t: T): Column = {
    val ser = CatalystSerializer[T]
    new Column(Literal.create(ser.toInternalRow(t), ser.schema))
  }

}
