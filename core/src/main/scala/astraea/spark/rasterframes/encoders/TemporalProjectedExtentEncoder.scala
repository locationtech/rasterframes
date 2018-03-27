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

import java.time.ZonedDateTime

import astraea.spark.rasterframes._
import geotrellis.spark.TemporalProjectedExtent
import geotrellis.vector.ProjectedExtent
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

/**
 * Custom encoder for [[ProjectedExtent]]. Necessary because [[geotrellis.proj4.CRS]] within [[ProjectedExtent]] isn't a case class, and [[ZonedDateTime]] doesn't have a natural encoder.
 *
 * @since 8/2/17
 */
object TemporalProjectedExtentEncoder {
  def apply(): ExpressionEncoder[TemporalProjectedExtent] = {
    DelegatingSubfieldEncoder(
      "extent" -> extentEncoder,
      "crs" -> crsEncoder,
      "instant" -> Encoders.scalaLong.asInstanceOf[ExpressionEncoder[Long]]
    )
  }
}
