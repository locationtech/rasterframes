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

package org.locationtech.rasterframes.extensions

import geotrellis.spark.{SpatialKey, TileLayerMetadata}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.broadcast
import org.locationtech.rasterframes._
import org.locationtech.rasterframes.util._
object ReprojectToLayer {

  def apply(df: DataFrame, tlm: TileLayerMetadata[SpatialKey]): RasterFrameLayer = {
    // create a destination dataframe with crs and extend columns
    // use RasterJoin to do the rest.
    val gb = tlm.gridBounds
    val crs = tlm.crs

    val gridItems = for {
      (col, row) <- gb.coordsIter
      sk = SpatialKey(col, row)
      e = tlm.mapTransform(sk)
    } yield (sk, e, crs)

    val dest = df.sparkSession.createDataFrame(gridItems.toSeq)
      .toDF(SPATIAL_KEY_COLUMN.columnName, EXTENT_COLUMN.columnName, CRS_COLUMN.columnName)
    val joined = RasterJoin(broadcast(dest), df)

    joined.asLayer(SPATIAL_KEY_COLUMN, tlm)
  }
}
