/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2018 Astraea. Inc.
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
 *
 */

package astraea.spark.rasterframes.experimental.datasource.awspds

import astraea.spark.rasterframes.StandardColumns._
import astraea.spark.rasterframes.util._
import astraea.spark.rasterframes.encoders.StandardEncoders
import org.apache.spark.sql.jts.JTSTypes
import org.apache.spark.sql.types._

/**
 * Standard column names
 *
 * @since 8/21/18
 */
trait PDSFields {
  final val PRODUCT_ID = StructField("product_id", StringType, false)
  final val ENTITY_ID = StructField("entity_id", StringType, false)
  final val ACQUISITION_DATE = StructField("acquisition_date", TimestampType, false)
  final val TIMESTAMP = StructField(TIMESTAMP_COLUMN.columnName, TimestampType, false)
  final val GRANULE_ID = StructField("granule_id", StringType, false)
  final val DOWNLOAD_URL = StructField("download_url", StringType, false)
  final val GID = StructField("gid", StringType, false)
  final val CLOUD_COVER = StructField("cloud_cover_pct", FloatType, false)
  final val PROC_LEVEL = StructField("processing_level", StringType, false)
  final val PATH = StructField("path", ShortType, false)
  final val ROW = StructField("row", ShortType, false)
  final val BOUNDS = StructField(BOUNDS_COLUMN.columnName, JTSTypes.GeometryTypeInstance, false)
  final def BOUNDS_WGS84 = StructField(
    "bounds_wgs84", StandardEncoders.envelopeEncoder.schema, false
  )
}

object PDSFields extends PDSFields
