/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea. Inc.
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
 *
 */

package astraea.spark.rasterframes.rules

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.sql.sources.Filter

/**
 * New filter types captured and rewritten for use in spatiotemporal data sources that can handle them.
 *
 * @since 1/11/18
 */
object SpatialFilters {
  case class Intersects(attribute: String, value: Geometry) extends Filter {
    def references: Array[String] = Array(attribute)
  }

  case class Contains(attribute: String, value: Geometry) extends Filter {
    def references: Array[String] = Array(attribute)
  }
}
