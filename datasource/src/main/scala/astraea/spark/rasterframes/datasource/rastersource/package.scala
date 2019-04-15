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

package astraea.spark.rasterframes.datasource
import java.net.URI

import org.apache.spark.sql.DataFrameReader
import shapeless.tag
import shapeless.tag.@@
package object rastersource {

  trait RasterSourceDataFrameReaderTag
  type RasterSourceDataFrameReader = DataFrameReader @@ RasterSourceDataFrameReaderTag

  /** Adds `rastersource` format specifier to `DataFrameReader`. */
  implicit class DataFrameReaderHasRasterSourceFormat(val reader: DataFrameReader) {
    def rastersource: RasterSourceDataFrameReader =
      tag[RasterSourceDataFrameReaderTag][DataFrameReader](
        reader.format(RasterSourceDataSource.SHORT_NAME))
  }

  /** Adds option methods relevant to RasterSourceDataSource. */
  implicit class RasterSourceDataFrameReaderHasOptions(val reader: RasterSourceDataFrameReader) {
    /** Set the zero-based band indexes to read. Defaults to Seq(0). */
    def withBandIndexes(bandIndexes: Int*): RasterSourceDataFrameReader =
      tag[RasterSourceDataFrameReaderTag][DataFrameReader](
        reader.option(RasterSourceDataSource.BAND_INDEXES_PARAM, bandIndexes.mkString(",")))

    def withTileDimensions(cols: Int, rows: Int): RasterSourceDataFrameReader =
      tag[RasterSourceDataFrameReaderTag][DataFrameReader](
        reader.option(RasterSourceDataSource.TILE_DIMS_PARAM, s"$cols,$rows")
      )

    def from(newlineDelimPaths: String): RasterSourceDataFrameReader =
      tag[RasterSourceDataFrameReaderTag][DataFrameReader](
        reader.option(RasterSourceDataSource.PATHS_PARAM, newlineDelimPaths)
      )

    def from(paths: Seq[String]): RasterSourceDataFrameReader =
      from(paths.mkString("\n"))

    def from(uris: Seq[URI])(implicit d: DummyImplicit): RasterSourceDataFrameReader =
      from(uris.map(_.toASCIIString))
  }
}
