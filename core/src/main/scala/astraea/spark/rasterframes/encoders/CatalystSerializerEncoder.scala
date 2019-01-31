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

package astraea.spark.rasterframes.encoders
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.reflect.runtime.universe.TypeTag

object CatalystSerializerEncoder {

  case class CatSerializeToRow[T](child: Expression, serde: CatalystSerializer[T])
    extends UnaryExpression with CodegenFallback {
    override def dataType: DataType = serde.schema
    override protected def nullSafeEval(input: Any): Any = {
      val value = input.asInstanceOf[T]
      serde.toInternalRow(value)
    }
  }
  case class CatDeserializeFromRow[T](child: Expression, serde: CatalystSerializer[T], outputType: DataType)
    extends UnaryExpression with CodegenFallback {
    override def dataType: DataType = outputType
    override protected def nullSafeEval(input: Any): Any = {
      val row = input.asInstanceOf[InternalRow]
      serde.fromInternalRow(row)
    }
  }
  def apply[T: TypeTag: CatalystSerializer]: ExpressionEncoder[T] = {
    val serde = CatalystSerializer[T]

    val schema = StructType(Seq(
      StructField("value", serde.schema)
    ))

    val parentType: DataType = ScalaReflection.dataTypeFor[T]

    val inputObject = BoundReference(0, parentType, nullable = false)

    val serializer = Seq(CatSerializeToRow(inputObject, serde))

    val deserializer: Expression = CatDeserializeFromRow(GetColumnByOrdinal(0, schema), serde, parentType)

    ExpressionEncoder(schema, flat = false, serializer, deserializer, typeToClassTag[T])
  }
}
