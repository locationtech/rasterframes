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

package astraea.spark.rasterframes.expressions
import astraea.spark.rasterframes._
import astraea.spark.rasterframes.ref.LayerSpace
import com.typesafe.scalalogging.LazyLogging
import geotrellis.spark.SpatialKey
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.rf._
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Projects/tiles incoming RasterRefs into tiles of a consistent layer.
 *
 * @since 9/7/18
 */
case class ProjectIntoLayer(children: Seq[Expression], space: LayerSpace) extends Expression
  with Generator with CodegenFallback with ExpectsInputTypes with LazyLogging {
  import astraea.spark.rasterframes.util._

  private val rrType = new RasterRefUDT()
  private val tType = new TileUDT()

  override def elementSchema: StructType = StructType(Seq(
    StructField(SPATIAL_KEY_COLUMN.columnName, spatialKeyEncoder.schema, false),
    StructField(BOUNDS_COLUMN.columnName, extentEncoder.schema, false)
  ) ++ children.map(e ⇒ StructField(e.name, tType, true)))

  override def inputTypes = Seq.fill(children.size)(rrType)

  override def nodeName: String = "projectIntoLayer"

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    val mapTransform = space.layout.mapTransform
    val tiles = for {
      child ← children
      element = child.eval(input)
      ref = rrType.deserialize(row(element))
      inExtent = ref.extent.reproject(ref.crs, space.crs)
      bounds = mapTransform(inExtent)
      (col, row) ← bounds.coordsIter
      outKey = SpatialKey(col, row)
      tile = ref.tile.reproject(space.crs).convert(space.cellType)
    } yield (child, outKey, tile)

    //InternalRow(spatialKeyEncoder.toRow(outKey), tType.serialize(tile))
    val grouped = tiles
      .groupBy(t ⇒ t._2)          // Group all tiles that have the same spatial key
      .mapValues(_.groupBy(_._1)) // Values as map on expression for later lookup
      .mapValues(_.mapValues(_.head._3)) // Drop the fields we no longer need
      .toSeq                      // ^^ This is where the merge method goes.
      .sortBy(_._1)               // Order on spatial key

    for {
      (key, map) ← grouped
      outExtent = mapTransform.keyToExtent(key)
      spCol = spatialKeyEncoder.toRow(key)
      extCol = extentEncoder.toRow(outExtent)
    } yield {
      val tiles = for {
        child ← children
        tile = map(child)
      } yield tType.serialize(tile)
      InternalRow(Seq(spCol, extCol) ++ tiles: _*)
    }
  }
}

object ProjectIntoLayer {
  def apply(rrs: Seq[Column], space: LayerSpace): Column =
    new ProjectIntoLayer(rrs.map(_.expr), space).asColumn
}
