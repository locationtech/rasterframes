package astraea.spark.rasterframes.rules

import astraea.spark.rasterframes.expressions.SpatialExpression
import org.apache.spark.sql.catalyst.expressions.ScalaUDF
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Swaps out spatial relation UDFs for expression forms.
 *
 * @since 2/19/18
 */
object SpatialUDFSubstitutionRules extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transform {
      case q: LogicalPlan ⇒ q.transformExpressions {
        case s: ScalaUDF ⇒ SpatialExpression.fromUDF(s).getOrElse(s)
      }
    }
  }
}
