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

package org.locationtech.rasterframes.expressions.generators

import geotrellis.raster._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, Generator, GenericInternalRow}
import org.apache.spark.sql.types._
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.expressions.DynamicExtractors
import org.locationtech.rasterframes.util._
import spire.syntax.cfor.cfor

/**
 * Catalyst expression for converting a tile column into a pixel column, with each tile pixel occupying a separate row.
 *
 * @since 4/12/17
 */
case class ExplodeTiles(
  sampleFraction: Double , seed: Option[Long], override val children: Seq[Expression])
  extends Expression with Generator with CodegenFallback {

  def this(children: Seq[Expression]) = this(1.0, None, children)
  override def nodeName: String = "rf_explode_tiles"

  override def elementSchema: StructType = {
    val names =
      if (children.size == 1) Seq("cell")
      else children.indices.map(i ⇒ s"cell_$i")

    StructType(
      Seq(
        StructField(COLUMN_INDEX_COLUMN.columnName, IntegerType, false),
        StructField(ROW_INDEX_COLUMN.columnName, IntegerType, false)) ++ names
        .map(n ⇒ StructField(n, DoubleType, false)))
  }

  private def sample[T](things: Seq[T]) = {
    // Apply random seed if provided
    seed.foreach(s ⇒ scala.util.Random.setSeed(s))
    scala.util.Random.shuffle(things)
      .take(math.ceil(things.length * sampleFraction).toInt)
  }

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    val tiles = Array.ofDim[Tile](children.length)
    cfor(0)(_ < tiles.length, _ + 1) { index =>
      val c = children(index)
      val row = c.eval(input).asInstanceOf[InternalRow]
      tiles(index) = if(row != null)
        DynamicExtractors.tileExtractor(c.dataType)(row)._1
      else null
    }
    val dims = tiles.filter(_ != null).map(_.dimensions)
    if(dims.isEmpty) Seq.empty[InternalRow]
    else {
      require(
        dims.distinct.length == 1,
        "Multi-column explode requires equally sized tiles. Found " + dims
      )

      val numOutCols = tiles.length + 2
      val (cols, rows) = dims.head

      val retval = Array.ofDim[InternalRow](cols * rows)
      cfor(0)(_ < rows, _ + 1) { row =>
        cfor(0)(_ < cols, _ + 1) { col =>
          val rowIndex = row * cols + col
          val outCols = Array.ofDim[Any](numOutCols)
          outCols(0) = col
          outCols(1) = row
          cfor(0)(_ < tiles.length, _ + 1) { index =>
            val tile = tiles(index)
            outCols(index + 2) = if(tile == null) doubleNODATA else tile.getDouble(col, row)
          }
          retval(rowIndex) = new GenericInternalRow(outCols)
        }
      }
      if(sampleFraction > 0.0 && sampleFraction < 1.0) sample(retval)
      else retval
    }
  }
}

object ExplodeTiles {
  def apply(sampleFraction: Double, seed: Option[Long], cols: Seq[Column]): Column = {
    val exploder = new ExplodeTiles(sampleFraction, seed, cols.map(_.expr))
    // Hack to grab the first two non-cell columns, containing the column and row indexes
    val metaNames = exploder.elementSchema.fieldNames.take(2)
    val colNames = cols.map(_.columnName)
    new Column(exploder).as(metaNames ++ colNames)
  }
}
