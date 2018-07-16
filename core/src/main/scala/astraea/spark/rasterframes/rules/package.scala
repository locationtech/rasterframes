package astraea.spark.rasterframes

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Module utilties.
 *
 * @since 2/19/18
 */
package object rules {
  def registerOptimization(sqlContext: SQLContext, rule: Rule[LogicalPlan]): Unit = {
    if(!sqlContext.experimental.extraOptimizations.contains(rule))
      sqlContext.experimental.extraOptimizations :+= rule
  }

  def register(sqlContext: SQLContext): Unit = {
    registerOptimization(sqlContext, SpatialUDFSubstitutionRules)
    registerOptimization(sqlContext, SpatialFilterPushdownRules)
  }
}
