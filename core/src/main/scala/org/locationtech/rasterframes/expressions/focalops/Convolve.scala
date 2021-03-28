///*
// * This software is licensed under the Apache 2 license, quoted below.
// *
// * Copyright 2020 Astraea, Inc.
// *
// * Licensed under the Apache License, Version 2.0 (the "License"); you may not
// * use this file except in compliance with the License. You may obtain a copy of
// * the License at
// *
// *     [http://www.apache.org/licenses/LICENSE-2.0]
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// * License for the specific language governing permissions and limitations under
// * the License.
// *
// * SPDX-License-Identifier: Apache-2.0
// *
// */
//
//package org.locationtech.rasterframes.expressions.focalops
//import geotrellis.raster.Tile
//import geotrellis.raster.mapalgebra.focal.Kernel
//import org.apache.spark.sql.{Column, TypedColumn}
//import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
//import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
//import org.apache.spark.sql.types.DataType
//import org.locationtech.rasterframes.expressions.{NullToValue, UnaryRasterOp}
//import org.locationtech.rasterframes.model.TileContext
//
//@ExpressionDescription(
//  usage = "_FUNC_(tile, kernel) - ",
//  arguments = """
//  Arguments:
//    * tile -
//    * kernel - """,
//  examples = ""
//  Examples:
//    > SELECT  _FUNC_(tile, Square(1));
//       ..."""
//)
//case class Convolve(child: Expression, kernel: Kernel) extends UnaryRasterOp with NullToValue with CodegenFallback {
//  override def nodeName: String = "rf_convolve"
//  override def na: Any = null
//  override def eval(tile: Tile, ctx: Option[TileContext]): Tile = tile.convolve(kernel)
//
//  def dataType: DataType = ???
//}
//
//object Convolve {
//  def apply(tile: Column, kernel: Kernel): Column = new Column(Convolve(tile.expr, kernel))
//}