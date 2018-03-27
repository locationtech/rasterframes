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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, InvokeLike}
import org.apache.spark.sql.types.DataType

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Module utilities
 *
 * @author sfitch 
 * @since 9/25/17
 */
package object encoders {
  private[rasterframes] def runtimeClass[T: TypeTag]: Class[T] =
    typeTag[T].mirror.runtimeClass(typeTag[T].tpe).asInstanceOf[Class[T]]

  private[rasterframes] def typeToClassTag[T: TypeTag]: ClassTag[T] = {
    ClassTag[T](typeTag[T].mirror.runtimeClass(typeTag[T].tpe))
  }

  /** Spark 2.1.0 -> 2.2.0 compatibility shim */
  def InvokeSafely(targetObject: Expression, functionName: String, dataType: DataType): InvokeLike = {
    val ctor = classOf[Invoke].getConstructors.head
    val TRUE = Boolean.box(true)
    if(ctor.getParameterTypes.length == 5) {
      // In Spark 2.1.0 the signature looks like this:
      // case class Invoke(
      //   targetObject: Expression,
      //   functionName: String,
      //   dataType: DataType,
      //   arguments: Seq[Expression] = Nil,
      //   propagateNull: Boolean = true) extends InvokeLike
      ctor.newInstance(targetObject, functionName, dataType, Nil, TRUE).asInstanceOf[InvokeLike]
    }
    else  {
      // In spark 2.2.0 the signature looks like this:
      // case class Invoke(
      //   targetObject: Expression,
      //   functionName: String,
      //   dataType: DataType,
      //   arguments: Seq[Expression] = Nil,
      //   propagateNull: Boolean = true,
      //   returnNullable : Boolean = true) extends InvokeLike
      ctor.newInstance(targetObject, functionName, dataType, Nil, TRUE, TRUE).asInstanceOf[InvokeLike]
    }
  }

}
