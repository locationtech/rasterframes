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

/**
 * Key constants associated with DataFrameReader options for certain DataSource implementations.
 *
 * @since 7/18/18
 */
trait DataSourceOptions {
  final val PATH_PARAM = "path"
  final val IMAGE_WIDTH_PARAM = "imageWidth"
  final val IMAGE_HEIGHT_PARAM = "imageWidth"
  final val TILE_SUBDIVISIONS_PARAM = "tileSubdivisions"
  final val NUM_PARTITIONS_PARAM = "numPartitions"
  final val LAYER_PARAM = "layer"
  final val ZOOM_PARAM = "zoom"
  final val TILE_COLUMN_PARAM = "tileColumn"
}

object DataSourceOptions extends DataSourceOptions
