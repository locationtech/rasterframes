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

package astraea.spark.rasterframes.datasource.geotiff

import java.net.URI

import astraea.spark.rasterframes._
import astraea.spark.rasterframes.datasource.geotiff.GeoTiffCollectionRelation.Cols
import astraea.spark.rasterframes.encoders.CatalystSerializer
import astraea.spark.rasterframes.util._
import geotrellis.proj4.CRS
import geotrellis.spark.io.hadoop.HadoopGeoTiffRDD
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.rf.TileUDT
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

/**
 *
 *
 * @since 7/31/18
 */
case class GeoTiffCollectionRelation(sqlContext: SQLContext, uri: URI, bandCount: Int) extends BaseRelation with PrunedScan {

  override def schema: StructType = StructType(Seq(
    StructField(Cols.PATH, StringType, false),
    StructField(EXTENT_COLUMN.columnName, CatalystSerializer[Extent].schema, nullable = true),
    StructField(CRS_COLUMN.columnName, CatalystSerializer[CRS].schema, false)
//    StructField(METADATA_COLUMN.columnName,
//      DataTypes.createMapType(StringType, StringType, false)
//    )
  ) ++ (
    if(bandCount == 1) Seq(StructField(Cols.TL, new TileUDT, false))
    else for(b ← 1 to bandCount) yield StructField(Cols.TL + "_" + b, new TileUDT, nullable = true)
  ))

  val keyer = (u: URI, e: ProjectedExtent) ⇒ (u.getPath, e)

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    implicit val sc = sqlContext.sparkContext

    val columnIndexes = requiredColumns.map(schema.fieldIndex)



    HadoopGeoTiffRDD.multiband(new Path(uri.toASCIIString), keyer, HadoopGeoTiffRDD.Options.DEFAULT)
      .map { case ((path, pe), mbt) ⇒
        val entries = columnIndexes.map {
          case 0 ⇒ path
          case 1 ⇒ CatalystSerializer[Extent].toRow(pe.extent)
          case 2 ⇒ CatalystSerializer[CRS].toRow(pe.crs)
          case i if i > 2 ⇒ {
            if(bandCount == 1 && mbt.bandCount > 2) mbt.color()
            else mbt.band(i - 3)
          }
        }
        Row(entries: _*)
      }

  }
}

object GeoTiffCollectionRelation {
  object Cols {
    lazy val PATH = "path"
    lazy val CRS = "crs"
    lazy val EX = BOUNDS_COLUMN.columnName
    lazy val TL = TILE_COLUMN.columnName
  }
}
