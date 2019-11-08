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

package org.locationtech.rasterframes.util

import geotrellis.raster.render.ColorRamps
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{base64, concat, concat_ws, length, lit, substring, when}
import org.apache.spark.sql.jts.JTSTypes
import org.apache.spark.sql.types.{StringType, StructField}
import org.locationtech.rasterframes.expressions.DynamicExtractors
import org.locationtech.rasterframes.{rfConfig, rf_render_png, rf_resample}
import org.apache.spark.sql.rf.WithTypeConformity

/**
  * DataFrame extension for rendering sample content in a number of ways
  */
trait DataFrameRenderers {
  private val truncateWidth = rfConfig.getInt("max-truncate-row-element-length")

  implicit class DFWithPrettyPrint(val df: Dataset[_]) {

    private def stringifyRowElements(cols: Seq[StructField], truncate: Boolean, renderTiles: Boolean) = {
      cols
        .map(c => {
          val resolved = df.col(s"`${c.name}`")
          if (renderTiles && DynamicExtractors.tileExtractor.isDefinedAt(c.dataType))
            concat(
              lit("<img src=\"data:image/png;base64,"),
              base64(rf_render_png(rf_resample(resolved, 0.5), ColorRamps.Viridis)), // TODO: how to expose?
              lit("\"></img>")
            )
          else {
            val isGeom = WithTypeConformity(c.dataType).conformsTo(JTSTypes.GeometryTypeInstance)
            val str = resolved.cast(StringType)
            if (truncate || isGeom)
              when(length(str) > lit(truncateWidth),
                concat(substring(str, 1, truncateWidth), lit("..."))
              )
              .otherwise(str)
            else str
          }
        })
    }

    def toMarkdown(numRows: Int = 5, truncate: Boolean = false, renderTiles: Boolean = true): String = {
      import df.sqlContext.implicits._
      val cols = df.schema.fields
      val header = cols.map(_.name).mkString("| ", " | ", " |") + "\n" + ("|---" * cols.length) + "|\n"
      val stringifiers = stringifyRowElements(cols, truncate, renderTiles)
      val cat = concat_ws(" | ", stringifiers: _*)
      val rows = df
        .select(cat)
        .limit(numRows)
        .as[String]
        .collect()
        .map(_.replaceAll("\\[", "\\\\["))
        .map(_.replace('\n', '↩'))

      val body = rows
        .mkString("| ", " |\n| ", " |")

      val caption = if (rows.length >= numRows) s"\n_Showing only top $numRows rows_.\n\n" else ""
      caption + header + body
    }

    def toHTML(numRows: Int = 5, truncate: Boolean = false, renderTiles: Boolean = true): String = {
      import df.sqlContext.implicits._
      val cols = df.schema.fields
      val header = "<thead>\n" + cols.map(_.name).mkString("<tr><th>", "</th><th>", "</th></tr>\n") + "</thead>\n"
      val stringifiers = stringifyRowElements(cols, truncate, renderTiles)
      val cat = concat_ws("</td><td>", stringifiers: _*)
      val rows = df
        .select(cat).limit(numRows)
        .as[String]
        .collect()

      val body = rows
        .mkString("<tr><td>", "</td></tr>\n<tr><td>", "</td></tr>\n")

      val caption = if (rows.length >= numRows) s"<caption>Showing only top $numRows rows</caption>\n" else ""

      "<table>\n" + caption + header + "<tbody>\n" + body + "</tbody>\n" + "</table>"
    }
  }
}
