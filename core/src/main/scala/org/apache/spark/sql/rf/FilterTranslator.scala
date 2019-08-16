/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.rf

import java.sql.{Date, Timestamp}

import org.locationtech.rasterframes.expressions.SpatialRelation.{Contains, Intersects}
import org.locationtech.rasterframes.rules._
import org.apache.spark.sql.catalyst.CatalystTypeConverters.{convertToScala, createToScalaConverter}
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{Attribute, EmptyRow, Expression, Literal}
import org.apache.spark.sql.jts.AbstractGeometryUDT
import org.apache.spark.sql.sources
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DateType, StringType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.geomesa.spark.jts.rules.GeometryLiteral
import org.locationtech.rasterframes.rules.{SpatialFilters, TemporalFilters}

/**
 * This is a copy of [[org.apache.spark.sql.execution.datasources.DataSourceStrategy.translateFilter]], modified to add our spatial predicates.
 *
 * @since 1/11/18
 */
object FilterTranslator {

  /**
   * Tries to translate a Catalyst [[Expression]] into data source [[Filter]].
   *
   * @return a `Some[Filter]` if the input [[Expression]] is convertible, otherwise a `None`.
   */
  def translateFilter(predicate: Expression): Option[Filter] = {
    predicate match {
      case Intersects(a: Attribute, Literal(geom, udt: AbstractGeometryUDT[_])) ⇒
        Some(SpatialFilters.Intersects(a.name, udt.deserialize(geom)))

      case Contains(a: Attribute, Literal(geom, udt: AbstractGeometryUDT[_])) ⇒
        Some(SpatialFilters.Contains(a.name, udt.deserialize(geom)))

      case Intersects(a: Attribute, GeometryLiteral(_, geom)) ⇒
        Some(SpatialFilters.Intersects(a.name, geom))

      case Contains(a: Attribute, GeometryLiteral(_, geom)) ⇒
        Some(SpatialFilters.Contains(a.name, geom))

      case expressions.And(
        expressions.GreaterThanOrEqual(a: Attribute, Literal(start, TimestampType)),
        expressions.LessThanOrEqual(b: Attribute, Literal(end, TimestampType))
      ) if a.name == b.name ⇒
        val toScala = createToScalaConverter(TimestampType)(_: Any).asInstanceOf[Timestamp]
        Some(TemporalFilters.BetweenTimes(a.name, toScala(start), toScala(end)))

      case expressions.And(
        expressions.GreaterThanOrEqual(a: Attribute, Literal(start, DateType)),
        expressions.LessThanOrEqual(b: Attribute, Literal(end, DateType))
      ) if a.name == b.name ⇒
        val toScala = createToScalaConverter(DateType)(_: Any).asInstanceOf[Date]
        Some(TemporalFilters.BetweenDates(a.name, toScala(start), toScala(end)))

      // TODO: Need to figure out how to generalize over capturing right-hand pairs
      case expressions.And(expressions.And(left,
        expressions.GreaterThanOrEqual(a: Attribute, Literal(start, TimestampType))),
        expressions.LessThanOrEqual(b: Attribute, Literal(end, TimestampType))
      ) if a.name == b.name ⇒
        val toScala = createToScalaConverter(TimestampType)(_: Any).asInstanceOf[Timestamp]

        for {
          leftFilter ← translateFilter(left)
          rightFilter = TemporalFilters.BetweenTimes(a.name, toScala(start), toScala(end))
        } yield sources.And(leftFilter, rightFilter)


      // TODO: Ditto as above
      case expressions.And(expressions.And(left,
        expressions.GreaterThanOrEqual(a: Attribute, Literal(start, DateType))),
        expressions.LessThanOrEqual(b: Attribute, Literal(end, DateType))
      ) if a.name == b.name ⇒
        val toScala = createToScalaConverter(DateType)(_: Any).asInstanceOf[Date]
        for {
          leftFilter ← translateFilter(left)
          rightFilter = TemporalFilters.BetweenDates(a.name, toScala(start), toScala(end))
        } yield sources.And(leftFilter, rightFilter)


      case expressions.EqualTo(a: Attribute, Literal(v, t)) =>
        Some(sources.EqualTo(a.name, convertToScala(v, t)))
      case expressions.EqualTo(Literal(v, t), a: Attribute) =>
        Some(sources.EqualTo(a.name, convertToScala(v, t)))

      case expressions.EqualNullSafe(a: Attribute, Literal(v, t)) =>
        Some(sources.EqualNullSafe(a.name, convertToScala(v, t)))
      case expressions.EqualNullSafe(Literal(v, t), a: Attribute) =>
        Some(sources.EqualNullSafe(a.name, convertToScala(v, t)))

      case expressions.GreaterThan(a: Attribute, Literal(v, t)) =>
        Some(sources.GreaterThan(a.name, convertToScala(v, t)))
      case expressions.GreaterThan(Literal(v, t), a: Attribute) =>
        Some(sources.LessThan(a.name, convertToScala(v, t)))

      case expressions.LessThan(a: Attribute, Literal(v, t)) =>
        Some(sources.LessThan(a.name, convertToScala(v, t)))
      case expressions.LessThan(Literal(v, t), a: Attribute) =>
        Some(sources.GreaterThan(a.name, convertToScala(v, t)))

      case expressions.GreaterThanOrEqual(a: Attribute, Literal(v, t)) =>
        Some(sources.GreaterThanOrEqual(a.name, convertToScala(v, t)))
      case expressions.GreaterThanOrEqual(Literal(v, t), a: Attribute) =>
        Some(sources.LessThanOrEqual(a.name, convertToScala(v, t)))

      case expressions.LessThanOrEqual(a: Attribute, Literal(v, t)) =>
        Some(sources.LessThanOrEqual(a.name, convertToScala(v, t)))
      case expressions.LessThanOrEqual(Literal(v, t), a: Attribute) =>
        Some(sources.GreaterThanOrEqual(a.name, convertToScala(v, t)))

      case expressions.InSet(a: Attribute, set) =>
        val toScala = createToScalaConverter(a.dataType)
        Some(sources.In(a.name, set.toArray.map(toScala)))

      // Because we only convert In to InSet in Optimizer when there are more than certain
      // items. So it is possible we still get an In expression here that needs to be pushed
      // down.
      case expressions.In(a: Attribute, list) if list.forall(_.isInstanceOf[Literal]) =>
        val hSet = list.map(e => e.eval(EmptyRow))
        val toScala = createToScalaConverter(a.dataType)
        Some(sources.In(a.name, hSet.toArray.map(toScala)))

      case expressions.IsNull(a: Attribute) =>
        Some(sources.IsNull(a.name))
      case expressions.IsNotNull(a: Attribute) =>
        Some(sources.IsNotNull(a.name))

      case expressions.And(left, right) =>
        // See SPARK-12218 for detailed discussion
        // It is not safe to just convert one side if we do not understand the
        // other side. Here is an example used to explain the reason.
        // Let's say we have (a = 2 AND trim(b) = 'blah') OR (c > 0)
        // and we do not understand how to convert trim(b) = 'blah'.
        // If we only convert a = 2, we will end up with
        // (a = 2) OR (c > 0), which will generate wrong results.
        // Pushing one leg of AND down is only safe to do at the top level.
        // You can see ParquetFilters' createFilter for more details.
        for {
          leftFilter <- translateFilter(left)
          rightFilter <- translateFilter(right)
        } yield sources.And(leftFilter, rightFilter)

      case expressions.Or(left, right) =>
        for {
          leftFilter <- translateFilter(left)
          rightFilter <- translateFilter(right)
        } yield sources.Or(leftFilter, rightFilter)

      case expressions.Not(child) =>
        translateFilter(child).map(sources.Not)

      case expressions.StartsWith(a: Attribute, Literal(v: UTF8String, StringType)) =>
        Some(sources.StringStartsWith(a.name, v.toString))

      case expressions.EndsWith(a: Attribute, Literal(v: UTF8String, StringType)) =>
        Some(sources.StringEndsWith(a.name, v.toString))

      case expressions.Contains(a: Attribute, Literal(v: UTF8String, StringType)) =>
        Some(sources.StringContains(a.name, v.toString))

      case _ => None
    }
  }
}
