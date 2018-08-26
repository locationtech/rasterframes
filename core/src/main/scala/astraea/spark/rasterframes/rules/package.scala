package astraea.spark.rasterframes

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
    registerOptimization(sqlContext, SpatialUDFSubstitutionRules)
    registerOptimization(sqlContext, SpatialFilterPushdownRules)
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
