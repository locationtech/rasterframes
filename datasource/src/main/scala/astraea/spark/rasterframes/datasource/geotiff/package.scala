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
 */

package astraea.spark.rasterframes.datasource

import java.net.URI

import astraea.spark.rasterframes._
import org.apache.spark.sql.{DataFrameReader, DataFrameWriter}
import shapeless.tag
import shapeless.tag.@@

/**
 * Extension methods enabled by this module.
 *
 * @since 1/16/18
 */
package object geotiff {
  /** Tagged type construction for enabling type-safe extension methods for loading
   * a RasterFrame in expected form. */
  type GeoTiffRasterFrameReader = DataFrameReader @@ GeoTiffRasterFrameReaderTag
  trait GeoTiffRasterFrameReaderTag
  /** Tagged type construction for enabling type-safe extension methods for writing
   * a RasterFrame to a GeoTIFF. */
  type GeoTiffRasterFrameWriter[T] = DataFrameWriter[T] @@ GeoTiffRasterFrameWriterTag
  trait GeoTiffRasterFrameWriterTag

  /** Adds `geotiff` format specifier to `DataFrameReader`. */
  implicit class DataFrameReaderHasGeoTiffFormat(val reader: DataFrameReader) {
    def geotiff: GeoTiffRasterFrameReader =
      tag[GeoTiffRasterFrameReaderTag][DataFrameReader](reader.format(DefaultSource.SHORT_NAME))
  }

  implicit class DataFrameWriterHasGeoTiffFormat[T](val writer: DataFrameWriter[T]) {
    def geotiff: GeoTiffRasterFrameWriter[T] =
      tag[GeoTiffRasterFrameWriterTag][DataFrameWriter[T]](writer.format(DefaultSource.SHORT_NAME))
  }

  /** Adds `loadRF` to appropriately tagged `DataFrameReader` */
  implicit class GeoTiffReaderWithRF(val reader: GeoTiffRasterFrameReader) {
    def loadRF(path: URI): RasterFrame = reader.load(path.toASCIIString).asRF
  }
}
