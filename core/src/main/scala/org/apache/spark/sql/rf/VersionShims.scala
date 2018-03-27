package org.apache.spark.sql.rf

import java.lang.reflect.Constructor

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, InvokeLike}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.DataType

/**
 * Collection of Spark version compatibility adapters.
 *
 * @since 2/13/18
 */
object VersionShims {
  def readJson(sqlContext: SQLContext, rows: Dataset[String]): DataFrame = {
    // NB: Will get a deprecation warning for Spark 2.2.x
    sqlContext.read.json(rows.rdd) // <-- deprecation warning expected
  }

  def updateRelation(lr: LogicalRelation, base: BaseRelation): LogicalPlan = {
    val lrClazz = classOf[LogicalRelation]
    val ctor = lrClazz.getConstructors.head.asInstanceOf[Constructor[LogicalRelation]]
    ctor.getParameterTypes.length match {
      // In Spark 2.1.0 the signature looks like this:
      //
      // case class LogicalRelation(
      //   relation: BaseRelation,
      //   expectedOutputAttributes: Option[Seq[Attribute]] = None,
      //   catalogTable: Option[CatalogTable] = None)
      //   extends LeafNode with MultiInstanceRelation
      // In Spark 2.2.0 it's like this:
      // case class LogicalRelation(
      //   relation: BaseRelation,
      //   output: Seq[AttributeReference],
      //   catalogTable: Option[CatalogTable])
      case 3 ⇒
        val arg2: Seq[AttributeReference] = lr.output
        val arg3: Option[CatalogTable] = lr.catalogTable
        if(ctor.getParameterTypes()(1).isAssignableFrom(classOf[Option[_]])) {
          ctor.newInstance(base, Option(arg2), arg3)
        }
        else {
          ctor.newInstance(base, arg2, arg3)
        }

      // In Spark 2.3.0 this signature is this:
      //
      // case class LogicalRelation(
      //   relation: BaseRelation,
      //   output: Seq[AttributeReference],
      //   catalogTable: Option[CatalogTable],
      //   override val isStreaming: Boolean)
      //   extends LeafNode with MultiInstanceRelation {
      case 4 ⇒
        val arg2: Seq[AttributeReference] = lr.output
        val arg3: Option[CatalogTable] = lr.catalogTable
        val arg4 = lrClazz.getMethod("isStreaming").invoke(lr)

        ctor.newInstance(base, arg2, arg3, arg4)
      case _ ⇒
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
      // In Spark 2.1.0 the signature looks like this:
      //
      // case class Invoke(
      //   targetObject: Expression,
      //   functionName: String,
      //   dataType: DataType,
      //   arguments: Seq[Expression] = Nil,
      //   propagateNull: Boolean = true) extends InvokeLike
      case 5 ⇒
        ctor.newInstance(targetObject, functionName, dataType, Nil, TRUE).asInstanceOf[InvokeLike]
      // In spark 2.2.0 the signature looks like this:
      //
      // case class Invoke(
      //   targetObject: Expression,
      //   functionName: String,
      //   dataType: DataType,
      //   arguments: Seq[Expression] = Nil,
      //   propagateNull: Boolean = true,
      //   returnNullable : Boolean = true) extends InvokeLike
      case 6 ⇒
        ctor.newInstance(targetObject, functionName, dataType, Nil, TRUE, TRUE).asInstanceOf[InvokeLike]

      case _ ⇒
        throw new NotImplementedError("Invoke constructor has unexpected shape")
    }
  }
}
