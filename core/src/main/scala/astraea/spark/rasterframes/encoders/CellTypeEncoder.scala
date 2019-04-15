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

package astraea.spark.rasterframes.encoders

import geotrellis.raster.{CellType, DataType}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.rf.VersionShims.InvokeSafely
import org.apache.spark.sql.types.{ObjectType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import CatalystSerializer._
import scala.reflect.classTag

/**
 * Custom encoder for GT [[CellType]]. It's necessary since [[CellType]] is a type alias of
 * a type intersection.
 * @since 7/21/17
 */
object CellTypeEncoder {
  def apply(): ExpressionEncoder[CellType] = {
    // We can't use StringBackedEncoder due to `CellType` being a type alias,
    // and Spark doesn't like that.
    import org.apache.spark.sql.catalyst.expressions._
    import org.apache.spark.sql.catalyst.expressions.objects._
    val ctType = ScalaReflection.dataTypeFor[DataType]
    val schema = schemaOf[CellType]
    val inputObject = BoundReference(0, ctType, nullable = false)

    val intermediateType = ObjectType(classOf[String])
    val serializer: Expression =
      StaticInvoke(
        classOf[UTF8String],
        StringType,
        "fromString",
        InvokeSafely(inputObject, "name", intermediateType) :: Nil
      )

    val inputRow = GetColumnByOrdinal(0, schema)
    val deserializer: Expression =
      StaticInvoke(CellType.getClass, ctType, "fromName", InvokeSafely(inputRow, "toString", intermediateType) :: Nil)

    ExpressionEncoder[CellType](schema, flat = false, Seq(serializer), deserializer, classTag[CellType])
  }
}
