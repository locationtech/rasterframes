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

package org.locationtech.rasterframes.experimental.datasource.awspds

import org.apache.spark.sql.types._

/**
 * Standard column names
 *
 * @since 8/21/18
 */
trait PDSFields {
  final val PRODUCT_ID = StructField("product_id", StringType, false)
  final val ACQUISITION_DATE = StructField("acquisition_date", TimestampType, false)
  final val GRANULE_ID = StructField("granule_id", StringType, false)
  final val DOWNLOAD_URL = StructField("download_url", StringType, false)
  final val GID = StructField("gid", StringType, false)
}

object PDSFields extends PDSFields
