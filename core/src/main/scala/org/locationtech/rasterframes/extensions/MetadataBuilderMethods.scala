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

import geotrellis.util.MethodExtensions
import org.apache.spark.sql.types.{Metadata, MetadataBuilder}
import org.locationtech.rasterframes.util._
import org.locationtech.rasterframes.{MetadataKeys, StandardColumns}

/**
 * Convenience to deal with boilerplate associated with adding
 * additional metadata to a column.
 *
 * @since 12/21/17
 */
private[rasterframes]
abstract class MetadataBuilderMethods extends MethodExtensions[MetadataBuilder] with MetadataKeys with StandardColumns {
  def attachContext(md: Metadata) = self.putMetadata(CONTEXT_METADATA_KEY, md)
  def tagSpatialKey = self.putString(SPATIAL_ROLE_KEY, SPATIAL_KEY_COLUMN.columnName)
  def tagTemporalKey = self.putString(SPATIAL_ROLE_KEY, TEMPORAL_KEY_COLUMN.columnName)
  def tagSpatialIndex = self.putString(SPATIAL_ROLE_KEY, SPATIAL_INDEX_COLUMN.columnName)
}
