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

package org.locationtech.rasterframes.datasource

import org.apache.spark.sql.DataFrameReader
import org.locationtech.rasterframes.datasource.raster.RasterSourceDataSource._
import shapeless.tag
import shapeless.tag.@@
package object raster {

  trait RasterSourceDataFrameReaderTag
  type RasterSourceDataFrameReader = DataFrameReader @@ RasterSourceDataFrameReaderTag

  /** Adds `raster` format specifier to `DataFrameReader`. */
  implicit class DataFrameReaderHasRasterSourceFormat(val reader: DataFrameReader) {
    def raster: RasterSourceDataFrameReader =
      tag[RasterSourceDataFrameReaderTag][DataFrameReader](
        reader.format(RasterSourceDataSource.SHORT_NAME))
  }

  /** Adds option methods relevant to RasterSourceDataSource. */
  implicit class RasterSourceDataFrameReaderHasOptions(val reader: RasterSourceDataFrameReader)
    extends CatalogReaderOptionsSupport[RasterSourceDataFrameReaderTag] with
      SpatialIndexOptionsSupport[RasterSourceDataFrameReaderTag]
}
