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

package org.locationtech.rasterframes.expressions.transformers

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, UnaryExpression}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.sql.{Column, TypedColumn}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.ref.RFRasterSource
import org.slf4j.LoggerFactory

import com.typesafe.scalalogging.Logger
import java.net.URI

/**
 * Catalyst generator to convert a geotiff download URL into a series of rows
 * containing references to the internal tiles and associated extents.
 *
 * @since 5/4/18
 */
case class URIToRasterSource(override val child: Expression)
  extends UnaryExpression with ExpectsInputTypes with CodegenFallback {
  @transient protected lazy val logger = Logger(LoggerFactory.getLogger(getClass.getName))


  override def nodeName: String = "rf_uri_to_raster_source"

  override def dataType: DataType = rasterSourceUDT

  override def inputTypes = Seq(StringType)

  override protected def nullSafeEval(input: Any): Any =  {
    val uriString = input.asInstanceOf[UTF8String].toString
    val uri = URI.create(uriString)
    val ref = RFRasterSource(uri)
    rasterSourceUDT.serialize(ref)
  }
}

object URIToRasterSource {
  def apply(rasterURI: Column): TypedColumn[Any, RFRasterSource] =
    new Column(new URIToRasterSource(rasterURI.expr)).as[RFRasterSource]
}
