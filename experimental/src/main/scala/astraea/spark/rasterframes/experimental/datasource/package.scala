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

package astraea.spark.rasterframes.experimental
import astraea.spark.rasterframes.encoders.SparkDefaultEncoders._
import astraea.spark.rasterframes.util._
import org.apache.spark.sql._
import org.apache.spark.sql.rf.CanBeColumn


/**
 *
 *
 * @since 9/3/18
 */
package object datasource {

  def download(urlColumn: Column): TypedColumn[Any, Array[Byte]] = {
    DownloadExpression(urlColumn.expr, urlColumn.columnName).asColumn
  }.as[Array[Byte]]

  def download_tiles(urlColumn: Column): Column =
    DownloadTilesExpression(urlColumn.expr, urlColumn.columnName).asColumn

  def raster_ref(rasterURIs: Seq[Column], useTiling: Boolean): Column =
    RasterRefExpression(rasterURIs.map(_.expr), useTiling).asColumn

  def cog_layout(rasterRefCol: Column): Column =
    COGLayoutExpression(rasterRefCol.expr).asColumn
}
