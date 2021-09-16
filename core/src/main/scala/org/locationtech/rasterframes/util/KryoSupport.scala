/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea, Inc.
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

package org.locationtech.rasterframes.util

import java.nio.ByteBuffer

import org.apache.spark.serializer.{KryoSerializer, SerializerInstance}
import org.apache.spark.{SparkConf, SparkEnv}

import scala.reflect.ClassTag

/**
 * Support for serialization via pooled kryo instances.
 *
 * @since 10/29/18
 */
object KryoSupport {
  @transient
  lazy val serializerPool: ThreadLocal[SerializerInstance] = new ThreadLocal[SerializerInstance]() {
    val ser: KryoSerializer = {
      val sparkConf =
        Option(SparkEnv.get)
          .map(_.conf)
          .orElse(Some(new SparkConf()))
          .map(_.set("spark.kryo.registrator", classOf[RFKryoRegistrator].getName))
          .get
      new KryoSerializer(sparkConf)
    }

    override def initialValue(): SerializerInstance = ser.newInstance()
  }

  def serialize[T: ClassTag](o: T): ByteBuffer = {
    val ser = serializerPool.get()
    ser.serialize(o)
  }

  def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    val ser = serializerPool.get()
    ser.deserialize(bytes)
  }
}


