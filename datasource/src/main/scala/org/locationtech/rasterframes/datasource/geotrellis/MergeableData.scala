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

package org.locationtech.rasterframes.datasource.geotrellis

trait MergeableData[D] {
  def merge(l:D, r:D): D
  def prototype(data:D): D
}

object MergeableData {
  def apply[D: MergeableData]:MergeableData[D] = implicitly[MergeableData[D]]

  implicit def MergeableString: MergeableData[String] = new MergeableData[String] {
    override def merge(l:String,r:String): String = (l,r) match {
      case("",str:String) => str
      case(str:String,"") => str
      case(l:String,r:String) => s"$l, $r"
    }
    override def prototype(data:String): String = ""
  }

  implicit def mergeableSeq[T]: MergeableData[Seq[T]] = new MergeableData[Seq[T]] {
    override def merge(l: Seq[T], r: Seq[T]): Seq[T] = l ++ r
    override def prototype(data: Seq[T]): Seq[T] = Seq.empty
  }

  implicit def mergeableSet[T]: MergeableData[Set[T]] = new MergeableData[Set[T]] {
    override def merge(l: Set[T], r: Set[T]): Set[T] = l ++ r
    override def prototype(data: Set[T]): Set[T] = Set.empty
  }

  // to be used as the value in a mergeableMap, the data type, V, must have context bound MergeableData as well
  implicit def mergeableMap[K,V: MergeableData]: MergeableData[Map[K,V]] = new MergeableData[Map[K,V]] {
    override def merge(l: Map[K,V], r: Map[K,V]): Map[K,V] = {
      (l.toSeq ++ r.toSeq)
        .groupBy{case(k,v) => k}
        .mapValues(_.map(_._2)
          .reduce[V] {case(lv,rv) => MergeableData[V].merge(lv,rv)})
    }
    override def prototype(data: Map[K,V]): Map[K,V] = Map.empty
  }
}
