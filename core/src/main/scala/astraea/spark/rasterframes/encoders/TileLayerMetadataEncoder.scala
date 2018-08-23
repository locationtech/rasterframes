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

package astraea.spark.rasterframes.encoders

import geotrellis.spark.{KeyBounds, TileLayerMetadata}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.reflect.runtime.universe._

/**
 * Specialized encoder for [[TileLayerMetadata]], necessary to be able to delegate to the
 * specialized cell type and crs encoders.
 *
 * @since 7/21/17
 */
object TileLayerMetadataEncoder {
  import astraea.spark.rasterframes._

  private def fieldEncoders = Seq[(String, ExpressionEncoder[_])](
    "cellType" -> cellTypeEncoder,
    "layout" -> layoutDefinitionEncoder,
    "extent" -> extentEncoder,
    "crs" -> crsEncoder
  )

  def apply[K: TypeTag](): ExpressionEncoder[TileLayerMetadata[K]] = {
    val boundsEncoder = ExpressionEncoder[KeyBounds[K]]()
    val fEncoders = fieldEncoders :+ ("bounds" -> boundsEncoder)
    DelegatingSubfieldEncoder(fEncoders: _*)
  }
}
