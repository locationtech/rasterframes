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

import geotrellis.raster.Dimensions
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.util.MethodExtensions
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.locationtech.rasterframes.encoders.StandardEncoders
import org.locationtech.rasterframes._

trait MultibandGeoTiffMethods extends MethodExtensions[MultibandGeoTiff] {
  def toDF(dims: Dimensions[Int] = NOMINAL_TILE_DIMS)(implicit spark: SparkSession): DataFrame = {
    val bands = self.bandCount
    val segmentLayout = self.imageData.segmentLayout
    val re = self.rasterExtent
    val crs = self.crs

    val windows = segmentLayout.listWindows(dims.cols, dims.rows)
    val subtiles = self.crop(windows)

    val rows = for {
      (gridbounds, tile) <- subtiles.toSeq
    } yield {
      val extent = re.extentFor(gridbounds, false)
      val extentRow =
        RowEncoder(StandardEncoders.extentEncoder.schema)
          .resolveAndBind()
          .createDeserializer()(StandardEncoders.extentEncoder.createSerializer()(extent))

      Row(extentRow +: crs +: tile.bands: _*)
    }

    val schema =
      StructType(
        Seq(
          StructField("extent", StandardEncoders.extentEncoder.schema, false),
          StructField("crs", crsUDT, false)
        ) ++ (1 to bands).map { i => StructField("b_" + i, tileUDT, false)}
      )

    spark.createDataFrame(spark.sparkContext.makeRDD(rows), schema)
  }
}
