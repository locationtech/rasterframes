package org.apache.spark.sql.rf

import java.lang.reflect.Constructor
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, FunctionRegistryBase}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.{FUNC_ALIAS, FunctionBuilder}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, InvokeLike}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.DataType

import scala.reflect._

/**
 * Collection of Spark version compatibility adapters.
 *
 * @since 2/13/18
 */
object VersionShims {
  def updateRelation(lr: LogicalRelation, base: BaseRelation): LogicalPlan = {
    val lrClazz = classOf[LogicalRelation]
    val ctor = lrClazz.getConstructors.head.asInstanceOf[Constructor[LogicalRelation]]
    ctor.getParameterTypes.length match {
      case 3 =>
        val arg2: Seq[AttributeReference] = lr.output
        val arg3: Option[CatalogTable] = lr.catalogTable
        if(ctor.getParameterTypes()(1).isAssignableFrom(classOf[Option[_]])) {
          ctor.newInstance(base, Option(arg2), arg3)
        }
        else {
          ctor.newInstance(base, arg2, arg3)
        }

      case 4 =>
        val arg2: Seq[AttributeReference] = lr.output
        val arg3: Option[CatalogTable] = lr.catalogTable
        val arg4 = lrClazz.getMethod("isStreaming").invoke(lr)

        ctor.newInstance(base, arg2, arg3, arg4)
      case _ =>
        throw new NotImplementedError("LogicalRelation constructor has unexpected shape")
    }
  }

  /**
   * Wraps [[Invoke]], using reflection to call different constructors for 2.1.0 and 2.2.0.
   */
  def InvokeSafely(targetObject: Expression, functionName: String, dataType: DataType): InvokeLike = {
    val ctor = classOf[Invoke].getConstructors.head
    val TRUE = Boolean.box(true)
    ctor.getParameterTypes.length match {
      case 5 =>
        ctor.newInstance(targetObject, functionName, dataType, Nil, TRUE).asInstanceOf[InvokeLike]
      case 6 =>
        ctor.newInstance(targetObject, functionName, dataType, Nil, TRUE, TRUE).asInstanceOf[InvokeLike]

      case _ =>
        throw new NotImplementedError("Invoke constructor has unexpected shape")
    }
  }

  implicit class RichFunctionRegistry(registry: FunctionRegistry) {

    def registerFunc(name: String, builder: FunctionRegistry.FunctionBuilder): Unit = {
      // Spark 2.3 introduced a new way of specifying Functions
      val spark23FI = "org.apache.spark.sql.catalyst.FunctionIdentifier"
      registry.getClass.getDeclaredMethods
        .filter(m => m.getName == "registerFunction" && m.getParameterCount == 2)
        .foreach { m =>
          val firstParam = m.getParameterTypes()(0)
          if(firstParam == classOf[String])
            m.invoke(registry, name, builder)
          else if(firstParam.getName == spark23FI) {
            val fic = Class.forName(spark23FI)
            val ctor = fic.getConstructor(classOf[String], classOf[Option[_]])
            val fi = ctor.newInstance(name, None).asInstanceOf[Object]
            m.invoke(registry, fi, builder)
          }
          else {
            throw new NotImplementedError("Unexpected FunctionRegistry API: " + m.toGenericString)
          }
        }
    }

    def registerExpression[T <: Expression : ClassTag](
      name: String,
      setAlias: Boolean = false,
      since: Option[String] = None
    ): (String, (ExpressionInfo, FunctionBuilder)) = {
      val (expressionInfo, builder) = FunctionRegistryBase.build[T](name, since)
      val newBuilder = (expressions: Seq[Expression]) => {
        val expr = builder(expressions)
        if (setAlias) expr.setTagValue(FUNC_ALIAS, name)
        expr
      }
      (name, (expressionInfo, newBuilder))
    }
  }
}
