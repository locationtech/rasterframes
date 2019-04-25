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

package org.locationtech.rasterframes.datasource.geotiff

import java.net.URI

import org.locationtech.rasterframes._
import org.locationtech.rasterframes.encoders.CatalystSerializer._
import org.locationtech.rasterframes.util._
import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.CRS
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop._
import geotrellis.util._
import geotrellis.vector.Extent
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.rf.TileUDT
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

/**
 * Spark SQL data source over a single GeoTiff file. Works best with CoG compliant ones.
 *
 * @since 1/14/18
 */
case class GeoTiffRelation(sqlContext: SQLContext, uri: URI) extends BaseRelation
  with PrunedScan with GeoTiffInfoSupport with LazyLogging  {

  lazy val (info, tileLayerMetadata) = extractGeoTiffLayout(
    HdfsRangeReader(new Path(uri), sqlContext.sparkContext.hadoopConfiguration)
  )

  def schema: StructType = {
    val skSchema = ExpressionEncoder[SpatialKey]().schema
    val skMetadata = Metadata.empty.append
      .attachContext(tileLayerMetadata.asColumnMetadata)
      .tagSpatialKey.build

    val baseName = TILE_COLUMN.columnName
    val tileCols = (if (info.bandCount == 1) Seq(baseName)
    else {
      for (i <- 0 until info.bandCount) yield s"${baseName}_${i + 1}"
    }).map(name ⇒
      StructField(name, new TileUDT, nullable = false)
    )

    StructType(Seq(
      StructField(SPATIAL_KEY_COLUMN.columnName, skSchema, nullable = false, skMetadata),
      StructField(EXTENT_COLUMN.columnName, schemaOf[Extent], nullable = true),
      StructField(CRS_COLUMN.columnName, schemaOf[CRS], nullable = true),
      StructField(METADATA_COLUMN.columnName,
        DataTypes.createMapType(StringType, StringType, false)
      )
    ) ++ tileCols)
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    logger.trace(s"Required columns: ${requiredColumns.mkString(", ")}")

    implicit val sc = sqlContext.sparkContext
    implicit val session = sqlContext.sparkSession
    val columnIndexes = requiredColumns.map(schema.fieldIndex)

    val tlm = tileLayerMetadata
    val trans = tlm.mapTransform
    val metadata = info.tags.headTags

    val encodedCRS = tlm.crs.toRow

    if(info.segmentLayout.isTiled) {
      // TODO: Figure out how to do tile filtering via the range reader.
      // Something with geotrellis.spark.io.GeoTiffInfoReader#windowsByPartition?
      HadoopGeoTiffRDD.spatialMultiband(new Path(uri), HadoopGeoTiffRDD.Options.DEFAULT)
        .map { case (pe, tiles) ⇒
          // NB: I think it's safe to take the min coord of the
          // transform result because the layout is directly from the TIFF
          val gb = trans.extentToBounds(pe.extent)
          val entries = columnIndexes.map {
            case 0 => SpatialKey(gb.colMin, gb.rowMin)
            case 1 => pe.extent.toRow
            case 2 => encodedCRS
            case 3 => metadata
            case n => tiles.band(n - 4)
          }
          Row(entries: _*)
        }
    }
    else {
      logger.warn("GeoTIFF is not already tiled. In-memory read required: " + uri)
      val geotiff = HadoopGeoTiffReader.readMultiband(new Path(uri))
      val rdd = sqlContext.sparkContext.makeRDD(Seq((geotiff.projectedExtent, Shims.toArrayTile(geotiff.tile))))

      rdd.tileToLayout(tlm)
        .map { case (sk, tiles) ⇒
          val entries = columnIndexes.map {
            case 0 => sk
            case 1 => trans.keyToExtent(sk).toRow
            case 2 => encodedCRS
            case 3 => metadata
            case n => tiles.band(n - 4)
          }
          Row(entries: _*)
        }
    }
  }
}
