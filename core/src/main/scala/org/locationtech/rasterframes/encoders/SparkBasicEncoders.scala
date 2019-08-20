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

package org.locationtech.rasterframes.encoders

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.reflect.runtime.universe._

/**
 * Container for primitive Spark encoders, pulled into implicit scope.
 *
 * @since 12/28/17
 */
private[rasterframes] trait SparkBasicEncoders {
  implicit def arrayEnc[T: TypeTag]: Encoder[Array[T]] = ExpressionEncoder()
  implicit val intEnc: Encoder[Int] = Encoders.scalaInt
  implicit val longEnc: Encoder[Long] = Encoders.scalaLong
  implicit val stringEnc: Encoder[String] = Encoders.STRING
  implicit val doubleEnc: Encoder[Double] = Encoders.scalaDouble
  implicit val boolEnc: Encoder[Boolean] = Encoders.scalaBoolean
}

object SparkBasicEncoders extends SparkBasicEncoders