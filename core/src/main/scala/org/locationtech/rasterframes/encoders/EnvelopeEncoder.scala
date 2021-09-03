/*
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

import org.locationtech.jts.geom.Envelope
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.objects.NewInstance
import org.apache.spark.sql.catalyst.expressions.{BoundReference, CreateNamedStruct, Literal}
import org.apache.spark.sql.rf.VersionShims.InvokeSafely
import org.apache.spark.sql.types._
import CatalystSerializer._
import scala.reflect.classTag

/**
 * Spark DataSet codec for JTS Envelope.
 *
 * @since 2/22/18
 */
object EnvelopeEncoder {

  val schema = schemaOf[Envelope]

  val dataType: DataType = ScalaReflection.dataTypeFor[Envelope]

  def apply(): ExpressionEncoder[Envelope] = {
    val inputObject = BoundReference(0, ObjectType(classOf[Envelope]), nullable = true)

    val invokers = schema.flatMap { f ⇒
      val getter = "get" + f.name.head.toUpper + f.name.tail
      Literal(f.name) :: InvokeSafely(inputObject, getter, DoubleType) :: Nil
    }

    val serializer = CreateNamedStruct(invokers)
    val deserializer = NewInstance(classOf[Envelope],
      (0 to 3).map(GetColumnByOrdinal(_, DoubleType)),
      dataType, false
    )

    new ExpressionEncoder[Envelope](serializer, deserializer, classTag[Envelope])
  }
}
*/
