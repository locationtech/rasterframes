/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright (c) 2021. Astraea, Inc.
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
 */

package org.locationtech.rasterframes.datasource

import org.apache.spark.sql.DataFrameWriter
import shapeless.tag.@@

package object tiles {
  trait TilesDataFrameReaderTag
  trait TilesDataFrameWriterTag

  type TilesDataFrameWriter[T] = DataFrameWriter[T] @@ TilesDataFrameWriterTag

  /** Adds `tiles` format specifier to `DataFrameWriter` */
  implicit class DataFrameWriterHasTilesWriter[T](val writer: DataFrameWriter[T]) {
    def tiles: TilesDataFrameWriter[T] =
      shapeless.tag[TilesDataFrameWriterTag][DataFrameWriter[T]](
        writer.format(TilesDataSource.SHORT_NAME))
  }

  /** Options for `tiles` format writer. */
  implicit class TilesWriterOps[T](val writer: TilesDataFrameWriter[T]) extends TilesWriterOptionsSupport[T]

  trait TilesWriterOptionsSupport[T] {
    val writer: TilesDataFrameWriter[T]

    /**
     * Provide the name of a column whose row value will be used as the output filename.
     * Generated value may have path components in it. Appropriate filename extension will be automatically added.
     *
     * @param colName name of column to use.
     */
    def withFilenameColumn(colName: String): TilesDataFrameWriter[T] = {
      shapeless.tag[TilesDataFrameWriterTag][DataFrameWriter[T]](
        writer.option(TilesDataSource.FILENAME_COL_PARAM, colName)
      )
    }

    /**
     * Enable generation of a `catalog.csv` file along with the tile filesf listing the file paths relative to
     * the base directory along with any identified metadata values vai `withMetadataColumns`.
     */
    def withCatalog: TilesDataFrameWriter[T] = {
      shapeless.tag[TilesDataFrameWriterTag][DataFrameWriter[T]](
        writer.option(TilesDataSource.CATALOG_PARAM, true.toString)
      )
    }

    /**
     * Specify column values to to add to chip metadata and catalog (when written).
     *
     * @param colNames names of columns to add. Values are automatically cast-ed to `String`
     */
    def withMetadataColumns(colNames: String*): TilesDataFrameWriter[T] = {
      shapeless.tag[TilesDataFrameWriterTag][DataFrameWriter[T]](
        writer.option(TilesDataSource.METADATA_PARAM, colNames.mkString(","))
      )
    }

    /** Request Tiles be written out in PNG format. GeoTIFF is the default. */
    def asPNG: TilesDataFrameWriter[T] = {
      shapeless.tag[TilesDataFrameWriterTag][DataFrameWriter[T]](
        writer.option(TilesDataSource.AS_PNG_PARAM, true.toString)
      )
    }
  }
}
