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

import java.net.URI
import java.util.UUID

import org.apache.spark.sql.{DataFrame, DataFrameReader}
import shapeless.tag
import shapeless.tag.@@
package object raster {

  private[raster] def tmpTableName() = UUID.randomUUID().toString.replace("-", "")

  trait RasterSourceDataFrameReaderTag
  type RasterSourceDataFrameReader = DataFrameReader @@ RasterSourceDataFrameReaderTag

  /** Adds `raster` format specifier to `DataFrameReader`. */
  implicit class DataFrameReaderHasRasterSourceFormat(val reader: DataFrameReader) {
    def raster: RasterSourceDataFrameReader =
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

    /** Indicate if tile reading should be delayed until cells are fetched. Defaults to `true`. */
    def withLazyTiles(state: Boolean): RasterSourceDataFrameReader =
      tag[RasterSourceDataFrameReaderTag][DataFrameReader](
        reader.option(RasterSourceDataSource.LAZY_TILES_PARAM, state))

    def fromCatalog(catalog: DataFrame, bandColumnNames: String*): RasterSourceDataFrameReader =
      tag[RasterSourceDataFrameReaderTag][DataFrameReader] {
        val tmpName = tmpTableName()
        catalog.createOrReplaceTempView(tmpName)
        reader
          .option(RasterSourceDataSource.CATALOG_TABLE_PARAM, tmpName)
          .option(RasterSourceDataSource.CATALOG_TABLE_COLS_PARAM, bandColumnNames.mkString(",")): DataFrameReader
      }

    def fromCatalog(tableName: String, bandColumnNames: String*): RasterSourceDataFrameReader =
      tag[RasterSourceDataFrameReaderTag][DataFrameReader](
        reader.option(RasterSourceDataSource.CATALOG_TABLE_PARAM, tableName)
          .option(RasterSourceDataSource.CATALOG_TABLE_COLS_PARAM, bandColumnNames.mkString(","))
      )

    def fromCSV(catalogCSV: String, bandColumnNames: String*): RasterSourceDataFrameReader =
      tag[RasterSourceDataFrameReaderTag][DataFrameReader](
        reader.option(RasterSourceDataSource.CATALOG_CSV_PARAM, catalogCSV)
          .option(RasterSourceDataSource.CATALOG_TABLE_COLS_PARAM, bandColumnNames.mkString(","))
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
