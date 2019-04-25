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

package org.locationtech.rasterframes.rules

import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.{BaseRelation, Filter}

/**
 * Interface for `LogicalRelation`s with the ability to capture spatial relations in query.
 *
 * @since 7/16/18
 */
trait SpatialRelationReceiver[+T <: SpatialRelationReceiver[T]] { self: BaseRelation ⇒
  /** Create new relation with the give filter added. */
  def withFilter(filter: Filter): T
  /** Check to see if relation already exists in this. */
  def hasFilter(filter: Filter): Boolean
  /** Type gymnastics helper. */
  def asBaseRelation: BaseRelation = self
}

object SpatialRelationReceiver {
  def unapply[T <: SpatialRelationReceiver[T]](lr: LogicalRelation): Option[SpatialRelationReceiver[T]] = lr.relation match {
    case t: SpatialRelationReceiver[T] @unchecked ⇒ Some(t)
    case _ ⇒ None
  }
}
