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

package org.apache.spark.sql.gt

import geotrellis.raster._
import org.apache.spark.sql.catalyst.analysis.MultiAlias
import org.apache.spark.sql.catalyst.expressions.{CreateArray, Expression, Inline}
import org.apache.spark.sql.types.{StructType, UDTRegistration, UserDefinedType}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Module of GeoTrellis UDTs for Spark/Catalyst.
 *
 * @author sfitch
 * @since 4/12/17
 */
package object types {

  /** Lookup the registered Catalyst UDT for the given Scala type. */
  def udtOf[T >: Null: TypeTag]: UserDefinedType[T] =
    UDTRegistration.getUDTFor(typeTag[T].tpe.toString).map(_.newInstance().asInstanceOf[UserDefinedType[T]])
      .getOrElse(throw new IllegalArgumentException(typeTag[T].tpe + " doesn't have a corresponding UDT"))


  /** Creates a Catalyst expression for flattening the fields in a struct into columns. */
  def projectStructExpression(dataType: StructType, input: Expression) =
    MultiAlias(Inline(CreateArray(Seq(input))), dataType.fields.map(_.name))

  val cellTypes: () ⇒ Seq[String] = () ⇒
    Seq(
      BitCellType,
      ByteCellType,
      ByteConstantNoDataCellType,
      UByteCellType,
      UByteConstantNoDataCellType,
      ShortCellType,
      ShortConstantNoDataCellType,
      UShortCellType,
      UShortConstantNoDataCellType,
      IntCellType,
      IntConstantNoDataCellType,
      FloatCellType,
      FloatConstantNoDataCellType,
      DoubleCellType,
      DoubleConstantNoDataCellType
    ).map(_.toString).distinct
}
