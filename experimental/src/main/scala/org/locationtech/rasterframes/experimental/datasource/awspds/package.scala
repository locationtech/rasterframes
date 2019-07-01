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

package org.locationtech.rasterframes.experimental.datasource

import org.apache.spark.sql.DataFrameReader
import shapeless.tag
import shapeless.tag.@@

/**
 * Module support.
 *
 * @since 5/4/18
 */
package object awspds {
  trait CatalogDataFrameReaderTag
  type CatalogDataFrameReader = DataFrameReader @@ CatalogDataFrameReaderTag

  implicit class DataFrameReaderHasL8CatalogFormat(val reader: DataFrameReader) {
    def l8Catalog: CatalogDataFrameReader =
      tag[CatalogDataFrameReaderTag][DataFrameReader](
        reader.format(L8CatalogDataSource.SHORT_NAME))

    def modisCatalog: CatalogDataFrameReader =
      tag[CatalogDataFrameReaderTag][DataFrameReader](
        reader.format(MODISCatalogDataSource.SHORT_NAME))
  }
}
