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

package org.locationtech.rasterframes.util

import org.locationtech.rasterframes._
import geotrellis.proj4.LatLng
import geotrellis.vector.{Feature, Geometry}
import geotrellis.vector.io.json.JsonFeatureCollection
import spray.json.JsValue

/**
 * Additional debugging routines. No guarantees these are or will remain stable.
 *
 * @since 4/6/18
 */
package object debug {
  implicit class RasterFrameWithDebug(val self: RasterFrame)  {

    /** Renders the whole schema with metadata as a JSON string. */
    def describeFullSchema: String = {
      self.schema.prettyJson
    }

    /** Renders all the extents in this RasterFrame as GeoJSON in EPSG:4326. This does a full
     * table scan and collects **all** the geometry into the driver, and then converts it into a
     * Spray JSON data structure. Not performant, and for debugging only. */
    def geoJsonExtents: JsValue = {
      import spray.json.DefaultJsonProtocol._

      val features = self
        .select(GEOMETRY_COLUMN, SPATIAL_KEY_COLUMN)
        .collect()
        .map{ case (p, s) ⇒ Feature(Geometry(p).reproject(self.crs, LatLng), Map("col" -> s.col, "row" -> s.row)) }

      JsonFeatureCollection(features).toJson
    }
  }
}
