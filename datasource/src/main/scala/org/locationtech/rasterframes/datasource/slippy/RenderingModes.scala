/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright (c) 2021. Astraea, Inc.
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
 */

package org.locationtech.rasterframes.datasource.slippy

object RenderingModes {
  // used as Enumeration of options in SlippyDataSource interpretation of string options.
  sealed trait RenderingMode
  case object Fast extends RenderingMode
  case object Uniform extends RenderingMode

  def renderingModeFromString(rendering_mode_name: String): RenderingMode =
    rendering_mode_name.toLowerCase() match {
      case "uniform" | "histogram" ⇒ Uniform
      case _ ⇒ Fast
    }

}