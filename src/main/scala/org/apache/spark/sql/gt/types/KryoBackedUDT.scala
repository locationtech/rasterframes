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

package org.apache.spark.sql.gt.types

import geotrellis.spark.util.KryoSerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

import scala.reflect.ClassTag

/**
 * Base class for UDTs who's contents is encoded using kryo
 *
 * @author sfitch
 * @since 4/18/17
 */
trait KryoBackedUDT[T >: Null] { self: UserDefinedType[T] ⇒

  implicit val targetClassTag: ClassTag[T]

  override val simpleString = typeName

  override def sqlType: DataType = StructType(Array(StructField(typeName + "_kryo", BinaryType)))

  override def userClass: Class[T] = targetClassTag.runtimeClass.asInstanceOf[Class[T]]

  override def serialize(obj: T): Any = {
    Option(obj)
      .map(KryoSerializer.serialize(_)(targetClassTag))
      .map(InternalRow.apply(_))
      .orNull
  }

  override def deserialize(datum: Any): T = {
    Option(datum)
      .collect { case row: InternalRow ⇒ row }
      .flatMap(row ⇒ Option(row.getBinary(0)))
      .map(KryoSerializer.deserialize[T](_)(targetClassTag))
      .orNull
  }
}
