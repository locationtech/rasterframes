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

package org.locationtech.rasterframes.tiles
import geotrellis.raster.{ArrayTile, DelegatingTile, Tile}

/**
 * Workaround for case where `combine` is invoked on two delegating tiles.
 * @since 8/22/18
 */
abstract class FixedDelegatingTile extends DelegatingTile {
  override def combine(r2: Tile)(f: (Int, Int) ⇒ Int): Tile = (delegate, r2) match {
    case (del: ArrayTile, r2: DelegatingTile) ⇒ del.combine(r2.toArrayTile())(f)
    case _ ⇒ delegate.combine(r2)(f)
  }

  override def combineDouble(r2: Tile)(f: (Double, Double) ⇒ Double): Tile = (delegate, r2) match {
    case (del: ArrayTile, r2: DelegatingTile) ⇒ del.combineDouble(r2.toArrayTile())(f)
    case _ ⇒ delegate.combineDouble(r2)(f)
  }
}
