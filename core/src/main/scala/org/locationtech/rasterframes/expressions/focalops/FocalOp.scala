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

import org.locationtech.rasterframes.expressions.DynamicExtractors.tileExtractor
import org.locationtech.rasterframes.expressions.{NullToValue, UnaryRasterOp, row}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback

trait FocalOp extends UnaryRasterOp with NullToValue with CodegenFallback {
  def na: Any = null

  override protected def nullSafeEval(input: Any): Any = {
    val (childTile, childCtx) = tileExtractor(child.dataType)(row(input))
    val result = op(extractBufferTile(childTile))
    toInternalRow(result, childCtx)
  }
}