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

package astraea.spark.rasterframes.datasource.geotiff

import java.net.URI

import astraea.spark.rasterframes._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.util._
import geotrellis.vector.Extent
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.gt.types.TileUDT
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import astraea.spark.rasterframes.util._
import geotrellis.raster.TileLayout
import geotrellis.raster.io.geotiff.MultibandGeoTiff

/**
 * Spark SQL data source over a single GeoTiff file. Works best with CoG compliant ones.
 *
 * @since 1/14/18
 */
case class GeoTiffRelation(sqlContext: SQLContext, uri: URI) extends BaseRelation with PrunedScan with LazyLogging  {

  lazy val info: GeoTiffReader.GeoTiffInfo = {
    GeoTiffReader.readGeoTiffInfo(
      HdfsRangeReader(new Path(uri), sqlContext.sparkContext.hadoopConfiguration),
      false, true
    )
  }

  val MAX_SIZE = 256
  def defaultLayout(cols: Int, rows: Int): TileLayout = {
    def divs(cells: Int) = {
      val layoutDivs = math.ceil(cells / MAX_SIZE.toFloat)
      val tileDivs = math.ceil(cells / layoutDivs)
      (layoutDivs.toInt, tileDivs.toInt)
    }
    val (layoutCols, tileCols) = divs(rows)
    val (layoutRows, tileRows) = divs(rows)
    TileLayout(layoutCols, layoutRows, tileCols, tileRows)
  }

  lazy val tileLayerMetadata: TileLayerMetadata[SpatialKey] = {
    val layout = if(!info.segmentLayout.isTiled) {
      val width = info.segmentLayout.totalCols
      val height = info.segmentLayout.totalRows
      defaultLayout(width, height)
    }
    else {
      info.segmentLayout.tileLayout
    }
    val extent = info.extent
    val crs = info.crs
    val cellType = info.cellType
    val bounds = KeyBounds(
      SpatialKey(0, 0),
      SpatialKey(layout.layoutCols - 1, layout.layoutRows - 1)
    )
    TileLayerMetadata(cellType, LayoutDefinition(extent, layout), extent, crs, bounds)

  }

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
      StructField(BOUNDS_COLUMN.columnName, org.apache.spark.sql.jts.JTSTypes.PolygonTypeInstance, nullable = true),
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

    if(info.segmentLayout.isTiled) {
      // TODO: Figure out how to do tile filtering via the range reader.
      // Something with geotrellis.spark.io.GeoTiffInfoReader#windowsByPartition?
      HadoopGeoTiffRDD.spatialMultiband(new Path(uri), HadoopGeoTiffRDD.Options.DEFAULT)
        .map { case (pe, tiles) ⇒
          // NB: I think it's safe to take the min coord of the
          // transform result because the layout is directly from the TIFF
          val gb = trans.extentToBounds(pe.extent)
          val entries = columnIndexes.map {
            case 0 ⇒ SpatialKey(gb.colMin, gb.rowMin)
            case 1 ⇒ pe.extent.jtsGeom
            case 2 ⇒ metadata
            case n ⇒ tiles.band(n - 3)
          }
          Row(entries: _*)
        }
    }
    else {
      logger.warn("GeoTIFF is not already tiled. In-memory read required: " + uri)
      val geotiff = HadoopGeoTiffReader.readMultiband(new Path(uri))
      val rdd = sqlContext.sparkContext.makeRDD(Seq((geotiff.projectedExtent, geotiff.tile)))

      rdd.tileToLayout(tlm)
        .map { case (sk, tiles) ⇒
          val entries = columnIndexes.map {
            case 0 ⇒ sk
            case 1 ⇒ trans.keyToExtent(sk).jtsGeom
            case 2 ⇒ metadata
            case n ⇒ tiles.band(n - 3)
          }
          Row(entries: _*)
        }
    }
  }
}
