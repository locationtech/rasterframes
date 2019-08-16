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

package org.locationtech.rasterframes.datasource.geotrellis

import java.net.URI

import org.locationtech.rasterframes.encoders.DelegatingSubfieldEncoder
import geotrellis.spark.LayerId
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.locationtech.rasterframes

/**
 *   /** Connector between a GT `LayerId` and the path in which it lives. */

 * @since 1/16/18
 */
case class Layer(base: URI, id: LayerId)

object Layer {
  def apply(base: URI, name: String, zoom: Int) = new Layer(base, LayerId(name, zoom))

  implicit def layerEncoder: ExpressionEncoder[Layer] = DelegatingSubfieldEncoder[Layer](
    "base" -> rasterframes.uriEncoder,
    "id" -> ExpressionEncoder[LayerId]()
  )
}
