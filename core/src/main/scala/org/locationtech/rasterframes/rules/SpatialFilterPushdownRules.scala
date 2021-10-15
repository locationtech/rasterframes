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

import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.rf.{FilterTranslator, VersionShims}

/**
 * Logical plan manipulations to handle spatial queries on tile components.
 *
 * @since 12/21/17
 */
object SpatialFilterPushdownRules extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformUp {
      case f @ Filter(condition, lr @ SpatialRelationReceiver(sr: SpatialRelationReceiver[_] @unchecked)) =>

        val preds = FilterTranslator.translateFilter(condition)

        def foldIt[T <: SpatialRelationReceiver[T]](rel: T): T =
          preds.foldLeft(rel)((r, f) => r.withFilter(f))

        preds.filterNot(sr.hasFilter).map(p => {
          val newRec = foldIt(sr)
          Filter(condition, VersionShims.updateRelation(lr, newRec.asBaseRelation))
        }).getOrElse(f)
    }
  }
}
