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

package astraea.spark.rasterframes.jts

import com.vividsolutions.jts.geom.{CoordinateSequence, Geometry}
import com.vividsolutions.jts.geom.util.GeometryTransformer
import geotrellis.proj4.CRS

/**
 * JTS Geometry reprojection transformation routine.
 *
 * @since 6/4/18
 */
class ReprojectionTransformer(src: CRS, dst: CRS) extends GeometryTransformer {
  lazy val transform = geotrellis.proj4.Transform(src, dst)
  override def transformCoordinates(coords: CoordinateSequence, parent: Geometry): CoordinateSequence = {
    val fact = parent.getFactory
    val retval = fact.getCoordinateSequenceFactory.create(coords)
    for(i <- 0 until coords.size()) {
      val x = coords.getOrdinate(i, CoordinateSequence.X)
      val y = coords.getOrdinate(i, CoordinateSequence.Y)
      val (xp, yp) = transform(x, y)
      retval.setOrdinate(i, CoordinateSequence.X, xp)
      retval.setOrdinate(i, CoordinateSequence.Y, yp)
    }
    retval
  }
}
