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

package astraea.spark.rasterframes.jts

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, ZonedDateTime}

import astraea.spark.rasterframes.expressions.SpatialRelation.{Contains, Intersects}
import org.locationtech.jts.geom._
import geotrellis.util.MethodExtensions
import geotrellis.vector.{Point ⇒ gtPoint}
import org.apache.spark.sql.{Column, TypedColumn}
import org.apache.spark.sql.functions._
import org.locationtech.geomesa.spark.jts.DataFrameFunctions.SpatialConstructors
import astraea.spark.rasterframes.encoders.StandardEncoders.PrimitiveEncoders._

/**
 * Extension methods on typed columns allowing for DSL-like queries over JTS types.
 * @since 1/10/18
 */
trait Implicits extends SpatialConstructors {
  implicit class ExtentColumnMethods[T <: Geometry](val self: TypedColumn[Any, T])
    extends MethodExtensions[TypedColumn[Any, T]] {

    def intersects(geom: Geometry): TypedColumn[Any, Boolean] =
      new Column(Intersects(self.expr, geomLit(geom).expr)).as[Boolean]

    def intersects(pt: gtPoint): TypedColumn[Any, Boolean] =
      new Column(Intersects(self.expr, geomLit(pt.jtsGeom).expr)).as[Boolean]

    def containsGeom(geom: Geometry): TypedColumn[Any, Boolean] =
      new Column(Contains(self.expr, geomLit(geom).expr)).as[Boolean]

  }

  implicit class PointColumnMethods(val self: TypedColumn[Any, Point])
    extends MethodExtensions[TypedColumn[Any, Point]] {

    def intersects(geom: Geometry): TypedColumn[Any, Boolean] =
      new Column(Intersects(self.expr, geomLit(geom).expr)).as[Boolean]
   }

  implicit class TimestampColumnMethods(val self: TypedColumn[Any, Timestamp])
    extends MethodExtensions[TypedColumn[Any, Timestamp]] {

    import scala.language.implicitConversions
    private implicit def zdt2ts(time: ZonedDateTime): Timestamp =
      new Timestamp(time.toInstant.toEpochMilli)
    private implicit def d2ts(date: Date): Timestamp =
      Timestamp.valueOf(date.toLocalDate.atTime(0, 0, 0))

    def betweenTimes(start: Timestamp, end: Timestamp): TypedColumn[Any, Boolean] =
      self.between(lit(start), lit(end)).as[Boolean]

    def betweenTimes(start: ZonedDateTime, end: ZonedDateTime): TypedColumn[Any, Boolean] =
      betweenTimes(start: Timestamp, end: Timestamp)

    def betweenDates(start: Date, end: Date): TypedColumn[Any, Boolean] =
      betweenTimes(start: Timestamp, end: Timestamp)
  }

  implicit class DateColumnMethods(val self: TypedColumn[Any, Date])
    extends MethodExtensions[TypedColumn[Any, Date]] {

    import scala.language.implicitConversions

    private implicit def ld2ts(date: LocalDate): Date = Date.valueOf(date)

    def betweenDates(start: Date, end: Date): TypedColumn[Any, Boolean] =
      self.between(lit(start), lit(end)).as[Boolean]

    def betweenDates(start: LocalDate, end: LocalDate): TypedColumn[Any, Boolean] =
      betweenDates(start: Date, end: Date)
  }
}

object Implicits extends Implicits
