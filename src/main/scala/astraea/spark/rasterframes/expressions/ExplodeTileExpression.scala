/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Astraea, Inc.
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

package astraea.spark.rasterframes.expressions

import astraea.spark.rasterframes
import geotrellis.raster.{NODATA, Tile}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, Generator}
import org.apache.spark.sql.gt.types.TileUDT
import org.apache.spark.sql.types._

/**
 * Catalyst expression for converting a tile column into a pixel column, with each tile pixel occupying a separate row.
 *
 * @author sfitch
 * @since 4/12/17
 */
private[rasterframes] case class ExplodeTileExpression(
  sampleFraction: Double = 1.0, override val children: Seq[Expression])
    extends Expression with Generator with CodegenFallback {

  override def elementSchema: StructType = {
    val names =
      if (children.size == 1) Seq("cell")
      else children.indices.map(i ⇒ s"cell_$i")

    StructType(
      Seq(
        StructField(rasterframes.COLUMN_INDEX_COLUMN, IntegerType, false),
        StructField(rasterframes.ROW_INDEX_COLUMN, IntegerType, false)) ++ names
        .map(n ⇒ StructField(n, DoubleType, false)))
  }

  private def keep(): Boolean = {
    if (sampleFraction >= 1.0) true
    else scala.util.Random.nextDouble() <= sampleFraction
  }

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    // Do we need to worry about deserializing all the tiles in a row like this?
    val tiles = for (child ← children)
      yield TileUDT.deserialize(child.eval(input).asInstanceOf[InternalRow])

    val dims = tiles.filter(_ != null).map(_.dimensions)
    if(dims.isEmpty) Seq.empty[InternalRow]
    else {
      require(
        dims.distinct.size == 1,
        "Multi-column explode requires equally sized tiles. Found " + dims
      )

      def safeGet(tile: Tile, col: Int, row: Int): Double =
        if (tile == null) NODATA else tile.getDouble(col, row)

      val (cols, rows) = dims.head

      for {
        row ← 0 until rows
        col ← 0 until cols
        if keep()
        contents = Seq[Any](col, row) ++ tiles.map(safeGet(_, col, row))
      } yield InternalRow(contents: _*)
    }
  }
}
