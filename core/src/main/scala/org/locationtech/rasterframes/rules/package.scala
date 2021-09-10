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

package org.locationtech.rasterframes

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.sources.{And, Filter}

/**
 * Rule registration support.
 *
 * @since 2/19/18
 */
package object rules {
  def registerOptimization(sqlContext: SQLContext, rule: Rule[LogicalPlan]): Unit = {
    if(!sqlContext.experimental.extraOptimizations.contains(rule))
      sqlContext.experimental.extraOptimizations :+= rule
  }

  def register(sqlContext: SQLContext): Unit = {
    org.locationtech.geomesa.spark.jts.rules.registerOptimizations(sqlContext)
    registerOptimization(sqlContext, SpatialUDFSubstitutionRules)
    // TODO: implement [[FilterTranslator]]
    // registerOptimization(sqlContext, SpatialFilterPushdownRules)
  }

  /** Separate And conditions into separate filters. */
  def splitFilters(f: Seq[Filter]): Seq[Filter] = {
    def splitConjunctives(f: Filter): Seq[Filter] =
    f match {
      case And(cond1, cond2) =>
        splitConjunctives(cond1) ++ splitConjunctives(cond2)
      case other => other :: Nil
    }
    f.flatMap(splitConjunctives)
  }
}
