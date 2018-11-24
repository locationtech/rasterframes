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

package astraea.spark.rasterframes.experimental
import org.apache.spark.sql._


/**
 * Module utilitities
 *
 * @since 9/3/18
 */
package object datasource {
  /** Downloads the referenced URL into an uninterpreted binary data array. */
  def download(urlColumn: Column): TypedColumn[Any, Array[Byte]] =
    DownloadExpression(urlColumn)

  /** Downloads the contents at each of the referenced URLs, interpreting
   * them as equally sized and  */
  def read_tiles(urls: Column*): Column =  DownloadTilesExpression(urls)
}
