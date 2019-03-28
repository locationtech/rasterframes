package org.apache.spark.sql.rf

import java.lang.reflect.{Constructor, Method}

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.{FunctionBuilder, expressionInfo}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, SQLContext}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BinaryExpression, Expression, ExpressionDescription, ExpressionInfo, RuntimeReplaceable, ScalaUDF}
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, InvokeLike}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.DataType

import scala.reflect._
import scala.util.{Failure, Success, Try}

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

  implicit class RichFunctionRegistry(registry: FunctionRegistry) {

    def registerFunc(name: String, builder: FunctionRegistry.FunctionBuilder): Unit = {
      // Spark 2.3 introduced a new way of specifying Functions
      val spark23FI = "org.apache.spark.sql.catalyst.FunctionIdentifier"
      registry.getClass.getDeclaredMethods
        .filter(m ⇒ m.getName == "registerFunction" && m.getParameterCount == 2)
        .foreach { m ⇒
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

    // Much of the code herein is copied from org.apache.spark.sql.catalyst.analysis.FunctionRegistry
    def registerExpression[T <: Expression: ClassTag](name: String): Unit = {
      val clazz = classTag[T].runtimeClass

      def expressionInfo: ExpressionInfo = {
        val df = clazz.getAnnotation(classOf[ExpressionDescription])
        if (df != null) {
          if (df.extended().isEmpty) {
            new ExpressionInfo(clazz.getCanonicalName, null, name, df.usage(), df.arguments(), df.examples(), df.note(), df.since())
          } else {
            // This exists for the backward compatibility with old `ExpressionDescription`s defining
            // the extended description in `extended()`.
            new ExpressionInfo(clazz.getCanonicalName, null, name, df.usage(), df.extended())
          }
        } else {
          new ExpressionInfo(clazz.getCanonicalName, name)
        }
      }
      def findBuilder: FunctionBuilder = {
        val constructors = clazz.getConstructors
        // See if we can find a constructor that accepts Seq[Expression]
        val varargCtor = constructors.find(_.getParameterTypes.toSeq == Seq(classOf[Seq[_]]))
        val builder = (expressions: Seq[Expression]) => {
          if (varargCtor.isDefined) {
            // If there is an apply method that accepts Seq[Expression], use that one.
            Try(varargCtor.get.newInstance(expressions).asInstanceOf[Expression]) match {
              case Success(e) => e
              case Failure(e) =>
                // the exception is an invocation exception. To get a meaningful message, we need the
                // cause.
                throw new AnalysisException(e.getCause.getMessage)
            }
          } else {
            // Otherwise, find a constructor method that matches the number of arguments, and use that.
            val params = Seq.fill(expressions.size)(classOf[Expression])
            val f = constructors.find(_.getParameterTypes.toSeq == params).getOrElse {
              val validParametersCount = constructors
                .filter(_.getParameterTypes.forall(_ == classOf[Expression]))
                .map(_.getParameterCount).distinct.sorted
              val expectedNumberOfParameters = if (validParametersCount.length == 1) {
                validParametersCount.head.toString
              } else {
                validParametersCount.init.mkString("one of ", ", ", " and ") +
                  validParametersCount.last
              }
              throw new AnalysisException(s"Invalid number of arguments for function ${clazz.getSimpleName}. " +
                s"Expected: $expectedNumberOfParameters; Found: ${params.length}")
            }
            Try(f.newInstance(expressions : _*).asInstanceOf[Expression]) match {
              case Success(e) => e
              case Failure(e) =>
                // the exception is an invocation exception. To get a meaningful message, we need the
                // cause.
                throw new AnalysisException(e.getCause.getMessage)
            }
          }
        }

        builder
      }

      registry.registerFunction(FunctionIdentifier(name), expressionInfo, findBuilder)
    }
  }
}
