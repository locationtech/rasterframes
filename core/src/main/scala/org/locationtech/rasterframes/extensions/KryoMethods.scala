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

package org.locationtech.rasterframes.extensions
import geotrellis.util.MethodExtensions
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.locationtech.rasterframes.util.RFKryoRegistrator

object KryoMethods {
  val kryoProperties = Map(
    "spark.serializer" -> classOf[KryoSerializer].getName,
    "spark.kryo.registrator" -> classOf[RFKryoRegistrator].getName,
    "spark.kryoserializer.buffer.max" -> "500m"
  )

  trait BuilderKryoMethods extends MethodExtensions[SparkSession.Builder] {
    def withKryoSerialization: SparkSession.Builder =
      kryoProperties.foldLeft(self) { case (bld, (key, value)) => bld.config(key, value) }
  }

  trait SparkConfKryoMethods extends MethodExtensions[SparkConf] {
    def withKryoSerialization: SparkConf = kryoProperties.foldLeft(self) {
      case (conf, (key, value)) => conf.set(key, value)
    }
  }
}
