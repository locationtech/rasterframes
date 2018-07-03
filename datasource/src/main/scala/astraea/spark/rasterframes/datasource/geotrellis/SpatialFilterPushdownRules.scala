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
 */

package astraea.spark.rasterframes.datasource.geotrellis

import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.rf.{FilterTranslator, VersionShims}

/**
 * Logical plan manipulations to handle spatial queries on tile components.
 * Starting points for understanding the Catalyst optimizer:
 * - [[org.apache.spark.sql.catalyst.trees.TreeNode]]
 * - [[org.apache.spark.sql.catalyst.expressions.Expression]]
 * - [[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan]]
 * - [[org.apache.spark.sql.catalyst.analysis.Analyzer]]
 * - [[org.apache.spark.sql.catalyst.optimizer.Optimizer]]
 *
 * @since 12/21/17
 */
object SpatialFilterPushdownRules extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformUp {
      case f @ Filter(condition, lr @ LogicalRelationWithGTR(gt: GeoTrellisRelation)) ⇒

        val preds = FilterTranslator.translateFilter(condition)

        preds.filterNot(gt.filters.contains).map(p ⇒ {
          val newGt = preds.foldLeft(gt)((r, f) ⇒ r.withFilter(f))
          Filter(condition, VersionShims.updateRelation(lr, newGt))
        }).getOrElse(f)
    }
  }
}
