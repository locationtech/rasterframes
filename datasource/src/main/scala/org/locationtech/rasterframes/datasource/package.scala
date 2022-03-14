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
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.locationtech.rasterframes

import cats.syntax.option._
import io.circe.Json
import io.circe.parser
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import sttp.model.Uri

import java.net.URI
import scala.util.Try

/**
 * Module utilities
 *
 * @since 1/13/18
 */
package object datasource {

  private[rasterframes]
  def numParam(key: String, parameters: Map[String, String]): Option[Long] =
    parameters.get(key).map(_.toLong)

  private[rasterframes]
  def numParam(key: String, parameters: CaseInsensitiveStringMap): Option[Long] =
    if(parameters.containsKey(key)) parameters.get(key).toLong.some
    else None

  private[rasterframes]
  def intParam(key: String, parameters: Map[String, String]): Option[Int] =
    parameters.get(key).map(_.toInt)

  private[rasterframes]
  def intParam(key: String, parameters: CaseInsensitiveStringMap): Option[Int] =
    if(parameters.containsKey(key)) Option(parameters.get(key)).map(_.toInt)
    else None

  private[rasterframes]
  def uriParam(key: String, parameters: Map[String, String]): Option[URI] =
    parameters.get(key).flatMap(p => Try(URI.create(p)).toOption)

  private[rasterframes]
  def uriParam(key: String, parameters: CaseInsensitiveStringMap): Option[Uri] =
    if(parameters.containsKey(key)) Uri.parse(parameters.get(key)).toOption
    else None

  private[rasterframes]
  def jsonParam(key: String, parameters: Map[String, String]): Option[Json] =
    parameters.get(key).flatMap(p => parser.parse(p).toOption)

  private[rasterframes]
  def jsonParam(key: String, parameters: CaseInsensitiveStringMap): Option[Json] =
    if(parameters.containsKey(key)) parser.parse(parameters.get(key)).toOption
    else None


  /**
   * Convenience grouping for transient columns defining spatial context.
   */
  private[rasterframes]
  case class SpatialComponents(crsColumn: Column,
                               extentColumn: Column,
                               dimensionColumn: Column,
                               cellTypeColumn: Column)

  private[rasterframes]
  object SpatialComponents {
    def apply(tileColumn: Column, crsColumn: Column, extentColumn: Column): SpatialComponents = {
      val dim = rf_dimensions(tileColumn) as "dims"
      val ct = rf_cell_type(tileColumn) as "cellType"
      SpatialComponents(crsColumn, extentColumn, dim, ct)
    }
    def apply(prColumn : Column): SpatialComponents = {
      SpatialComponents(
        rf_crs(prColumn) as "crs",
        rf_extent(prColumn) as "extent",
        rf_dimensions(prColumn) as "dims",
        rf_cell_type(prColumn) as "cellType"
      )
    }
  }

  /**
   * If the given DataFrame has extent and CRS columns return the DataFrame, the CRS column an extent column.
   * Otherwise, see if there's a `ProjectedRaster` column add `crs` and `extent` columns extracted from the
   * `ProjectedRaster` column to the returned DataFrame.
   *
   * @param d DataFrame to process.
   * @return Tuple containing the updated DataFrame followed by the CRS column and the extent column
   */
  private[rasterframes]
  def projectSpatialComponents(d: DataFrame): Option[SpatialComponents] =
    d.tileColumns.headOption.zip(d.crsColumns.headOption.zip(d.extentColumns.headOption)).headOption
      .map { case (tile, (crs, extent)) => SpatialComponents(tile, crs, extent) }
      .orElse(
        d.projRasterColumns.headOption
          .map(pr => SpatialComponents(pr))
      )
}
