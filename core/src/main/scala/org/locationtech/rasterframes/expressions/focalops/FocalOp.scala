/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2021 Astraea, Inc.
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

package org.locationtech.rasterframes.expressions.focalops

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.locationtech.rasterframes.expressions.DynamicExtractors.tileExtractor
import org.locationtech.rasterframes.expressions.{NullToValue, UnaryLocalRasterOp, row}
import org.locationtech.rasterframes.ref.RasterRef
import org.locationtech.rasterframes.tiles.ProjectedRasterTile

trait FocalOp extends UnaryLocalRasterOp with NullToValue with CodegenFallback {
  def na: Any = null

  override protected def nullSafeEval(input: Any): Any = {
    val (childTile, childCtx) = tileExtractor(child.dataType)(row(input))
    val literral = childTile match {
      // if it is RasterRef, we want the BufferTile
      case ref: RasterRef => ref.realizedTile
      // if it is a ProjectedRasterTile, can we flatten it?
      case prt: ProjectedRasterTile => prt.tile match {
        // if it is RasterRef, we can get what's inside
        case rr: RasterRef => rr.realizedTile
        // otherwise it is some tile
        case _             => prt
      }
    }
    val result = op(literral)
    toInternalRow(result, childCtx)
  }
}