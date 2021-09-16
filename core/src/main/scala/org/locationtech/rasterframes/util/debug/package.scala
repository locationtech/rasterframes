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

import java.lang.reflect.{AccessibleObject, Modifier}

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD

import scala.util.Try

/**
 * Additional debugging routines. No guarantees these are or will remain stable.
 *
 * @since 4/6/18
 */
package object debug {

  implicit class DescribeablePartition(val p: Partition) extends AnyVal {
    def describe: String = Try {
      def acc[A <: AccessibleObject](a: A): A = { a.setAccessible(true); a }

      val getters =
        p
          .getClass
          .getDeclaredMethods
          .filter(_.getParameterCount == 0)
          .filter(m => (m.getModifiers & Modifier.PUBLIC) > 0)
          .filterNot(_.getName == "hashCode")
          .map(acc)
          .map(m => m.getName + "=" + String.valueOf(m.invoke(p)))

      val fields =
        p
          .getClass
          .getDeclaredFields
          .filter(f => (f.getModifiers & Modifier.PUBLIC) > 0)
          .map(acc)
          .map(m => m.getName + "=" + String.valueOf(m.get(p)))

      p.getClass.getSimpleName + "(" + (fields ++ getters).mkString(", ") + ")"

    }.getOrElse(p.toString)
  }

  implicit class RDDWithPartitionDescribe(val r: RDD[_]) extends AnyVal {
    def describePartitions: String = r.partitions.map(p => ("Partition " + p.index) -> p.describe).mkString("\n")
  }
}
