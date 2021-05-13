/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2019 Astraea, Inc.
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

import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.types.{DataType, ObjectType, StructField, StructType}

import scala.reflect.runtime.universe.TypeTag

object CatalystSerializerEncoder {

  case class CatSerializeToRow[T](child: Expression, serde: CatalystSerializer[T])
    extends UnaryExpression {
    override def dataType: DataType = serde.schema
    override protected def nullSafeEval(input: Any): Any = {
      val value = input.asInstanceOf[T]
      serde.toInternalRow(value)
    }
    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
      val cs = ctx.addReferenceObj("serde", serde, serde.getClass.getName)
      nullSafeCodeGen(ctx, ev, input => s"${ev.value} = $cs.toInternalRow($input);")
    }
  }
  case class CatDeserializeFromRow[T](child: Expression, serde: CatalystSerializer[T], outputType: DataType)
    extends UnaryExpression {
    override def dataType: DataType = outputType

    private def objType = outputType match {
      case ot: ObjectType => ot.cls.getName
      case o => s"java.lang.Object /* $o */" // not sure what to do here... hopefully shouldn't happen
    }
    override protected def nullSafeEval(input: Any): Any = {
      val row = input.asInstanceOf[InternalRow]
      serde.fromInternalRow(row)
    }
    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
      val cs = ctx.addReferenceObj("serde", serde, classOf[CatalystSerializer[_]].getName)
      nullSafeCodeGen(ctx, ev, input => s"${ev.value} = ($objType) $cs.fromInternalRow($input);")
    }
  }
  def apply[T: TypeTag: CatalystSerializer](): ExpressionEncoder[T] = {
    val serde = CatalystSerializer[T]

    val schema = serde.schema

    val parentType: DataType = ScalaReflection.dataTypeFor[T]

    val inputObject = BoundReference(0, parentType, nullable = true)

    val serializer = CatSerializeToRow(inputObject, serde)

    val deserializer: Expression = CatDeserializeFromRow(GetColumnByOrdinal(0, schema), serde, parentType)

    ExpressionEncoder(serializer, deserializer, typeToClassTag[T])
  }
}
